# Stream Processor Plugin Metrics

## Overview

The Stream Processor Plugin provides essential metrics for monitoring stream processing performance and health. These metrics focus on the most critical aspects of operation: throughput, errors, and resource utilization.

## Essential Metrics

### Core Processing Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `messages_processed` | Counter | Total messages processed successfully |
| `messages_errored` | Counter | Messages that failed processing |
| `messages_dropped` | Counter | Messages dropped (no outputs generated) |
| `outputs_generated` | Counter | Total output messages generated |

**Purpose**: Monitor overall processing health and throughput.

**Usage**:
- **Throughput**: `messages_processed/second` indicates processing rate
- **Error Rate**: `messages_errored/(messages_processed + messages_errored)` shows failure percentage
- **Drop Rate**: `messages_dropped/(messages_processed + messages_dropped)` shows messages with no outputs
- **Output Ratio**: `outputs_generated/messages_processed` shows average outputs per input

### JavaScript Execution Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `javascript_errors` | Counter | JavaScript expression errors |
| `javascript_execution_time` | Timer | Time spent executing JavaScript expressions |

**Purpose**: Monitor JavaScript performance and reliability.

**Usage**:
- **JS Error Rate**: `javascript_errors/outputs_generated` shows expression failure rate
- **JS Performance**: `javascript_execution_time` percentiles show execution latency

### Performance Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `batch_processing_time` | Timer | Time to process entire batch |
| `message_processing_time` | Timer | Time to process individual message |

**Purpose**: Monitor processing performance and identify bottlenecks.

**Usage**:
- **Batch Latency**: `batch_processing_time` shows end-to-end batch processing time
- **Message Latency**: `message_processing_time` shows per-message processing time
- **Throughput**: `1/message_processing_time` estimates maximum messages per second

### Resource Utilization

| Metric | Type | Description |
|--------|------|-------------|
| `active_mappings` | Gauge | Number of active mappings (static + dynamic) |
| `active_variables` | Gauge | Number of active variables in state |

**Purpose**: Monitor resource usage and system scale.

**Usage**:
- **Configuration Scale**: `active_mappings` shows total configured mappings
- **State Scale**: `active_variables` shows variables being tracked
- **Memory Estimation**: Higher values indicate more memory usage

## Monitoring Dashboards

### Health Dashboard

**Key Metrics**:
- Messages processed per second
- Error rate percentage
- Drop rate percentage
- JavaScript error rate

**Alerts**:
- Error rate > 5%
- Drop rate > 10%
- JavaScript error rate > 1%

### Performance Dashboard

**Key Metrics**:
- Message processing time (p50, p95, p99)
- Batch processing time
- JavaScript execution time
- Output generation rate

**Alerts**:
- p95 message processing time > 100ms
- p99 JavaScript execution time > 50ms

### Resource Dashboard

**Key Metrics**:
- Active mappings count
- Active variables count
- Processing throughput trend

**Alerts**:
- Active variables > 10,000 (potential memory issue)

## Metric Collection

### Prometheus Format

```
# HELP stream_processor_messages_processed_total Total messages processed successfully
# TYPE stream_processor_messages_processed_total counter
stream_processor_messages_processed_total 12345

# HELP stream_processor_messages_errored_total Messages that failed processing
# TYPE stream_processor_messages_errored_total counter
stream_processor_messages_errored_total 23

# HELP stream_processor_javascript_execution_time_seconds Time spent executing JavaScript
# TYPE stream_processor_javascript_execution_time_seconds histogram
stream_processor_javascript_execution_time_seconds_bucket{le="0.001"} 1000
stream_processor_javascript_execution_time_seconds_bucket{le="0.01"} 1200
stream_processor_javascript_execution_time_seconds_bucket{le="0.1"} 1250
stream_processor_javascript_execution_time_seconds_bucket{le="+Inf"} 1250
```

### Usage in Code

```go
// Log successful processing
processor.metrics.LogMessageProcessed(processingTime)

// Log processing errors
processor.metrics.LogMessageErrored()

// Log JavaScript execution
processor.metrics.LogJavaScriptExecution(executionTime, success)

// Log output generation
processor.metrics.LogOutputGeneration(outputCount)
```

## Troubleshooting

### High Error Rate

**Symptoms**: `messages_errored` increasing rapidly
**Causes**:
- Invalid JavaScript expressions
- Malformed input messages
- Missing dependencies

**Investigation**:
1. Check logs for specific error messages
2. Verify JavaScript expression syntax
3. Validate input message format

### Poor Performance

**Symptoms**: High `message_processing_time` or `javascript_execution_time`
**Causes**:
- Complex JavaScript expressions
- Large number of mappings
- High variable count

**Investigation**:
1. Review JavaScript expression complexity
2. Check `active_mappings` and `active_variables` counts
3. Consider expression optimization

### High Drop Rate

**Symptoms**: High `messages_dropped` relative to `messages_processed`
**Causes**:
- Messages missing umh_topic metadata
- Topics not matching configured sources
- Invalid timeseries format

**Investigation**:
1. Verify input message format
2. Check source configuration
3. Review topic matching patterns

## Best Practices

1. **Monitor Continuously**: Set up dashboards and alerts for key metrics
2. **Baseline Performance**: Establish normal operating ranges for all metrics
3. **Correlate Metrics**: Use multiple metrics together to diagnose issues
4. **Regular Review**: Periodically review metric trends and adjust thresholds
5. **Keep It Simple**: Focus on actionable metrics rather than comprehensive coverage 