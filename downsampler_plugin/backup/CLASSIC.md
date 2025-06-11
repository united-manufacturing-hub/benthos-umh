# UMH Classic Format Support (Backup Documentation)

This document contains information about the UMH Classic format support that was temporarily removed from the downsampler plugin to focus on UMH-core format first.

## UMH Classic _historian Format

Multiple fields in one JSON object with shared timestamp, identified by `data_contract: "_historian"`.

### Format Example
```json
{
  "temperature": 23.4,
  "humidity": 42.1,
  "pressure": 1013.25,
  "timestamp_ms": 1609459200000
}
```

### Key Features for Classic Format
- **Per-Key Filtering**: Each metric field is evaluated independently against its specific configuration
- **Selective Field Removal**: Keys that don't meet their threshold are **removed** from the message
- **Intelligent Message Dropping**: Only if **all** measurement keys are dropped is the entire message dropped
- **Metadata Annotation**: Special metadata format showing filtering statistics like "downsampler(filtered_3_of_5_keys)"

### Classic Format Logic
For classic format messages, the downsampler would:
1. Identify all non-timestamp fields as separate data points
2. Process each field independently with its own series state
3. Remove fields that don't meet threshold criteria
4. Keep fields that do meet threshold criteria
5. Add metadata showing how many fields were filtered vs kept
6. Only drop the entire message if all measurement fields were filtered

### Files Moved to Backup
- `backup/downsampler_classic_test.go` - Complete test suite for classic format
- Classic format handling code sections from main plugin

### Future Recovery
When ready to re-add classic format support:
1. Review this documentation
2. Restore test file from backup
3. Re-implement per-key filtering logic in main plugin
4. Update metadata generation for classic format statistics
5. Ensure both formats work together harmoniously

## Classic Format Test Examples

The backup test file contains comprehensive examples including:
- Per-key filtering functionality
- Boolean and string handling in multi-field messages
- Mixed data types with different thresholds
- Series state management per field
- Metadata annotation with filtering statistics
- Format detection and processing logic

## Configuration for Classic Format

The configuration would remain the same, but the processing logic would need to handle:
- Multiple fields per message
- Independent series state per field
- Field-specific threshold application
- Selective field filtering
- Classic metadata format generation 