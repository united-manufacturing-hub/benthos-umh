# Beckhoff ADS Plugin - Developer Guide

Developer documentation for the Beckhoff ADS Benthos input plugin. For user documentation, see [docs/input/beckhoff-ads-community.md](../docs/input/beckhoff-ads-community.md).

## Overview

This plugin reads data from Beckhoff PLCs using the ADS (Automation Device Specification) protocol, built on top of the [`RuneRoven/go-ads`](https://github.com/RuneRoven/go-ads) library.

Key capabilities:
- Interval polling and notification-based reads
- Supports TwinCAT 2 (runtime port 801) and TwinCAT 3 (runtime port 851)
- Automatic UDP route registration on the PLC
- Docker/Kubernetes support without host networking
- Scalar types, structs, arrays, strings, enums, and time/date types

## File Structure

```
beckhoff_ads_plugin/
├── ads_input.go                        # Plugin implementation (Connect, ReadBatch, Close)
├── deterministic_helpers_test.go       # Exported formula helpers for PLC value verification
├── beckhoff_ads_plugin_suite_test.go   # Ginkgo test suite bootstrap
├── beckhoff_ads_test.go                # Hardware integration tests (TC2 + TC3)
├── deterministic_helpers_unit_test.go  # Unit tests for formula helpers and internal functions
└── README.md                           # This file
```

## Testing

### Unit Tests

Pure Go tests for deterministic formula helpers, symbol parsing, sanitization, and container IP detection. No hardware required.

```bash
TEST_ADS_UNITTEST=true make test-ads
```

### Hardware Integration Tests

Run against physical Beckhoff PLCs with the deterministic test PLC program (`tc3prg`). The tests verify correct reading of scalars, structs, arrays, strings, enums, and time types by comparing against expected values computed from the PLC's master cycle counter.

**TwinCAT 3 (CX7000):**
```bash
TEST_ADS_TC3_TARGET_IP="192.168.1.100" \
TEST_ADS_TC3_TARGET_AMS="192.168.1.100.1.1" \
TEST_ADS_TC3_RUNTIME_PORT=851 \
make test-ads
```

**TwinCAT 2 (CX1020):**
```bash
TEST_ADS_TC2_TARGET_IP="192.168.1.200" \
TEST_ADS_TC2_TARGET_AMS="192.168.1.200.1.1" \
TEST_ADS_TC2_RUNTIME_PORT=801 \
make test-ads
```

Tests are skipped automatically when the corresponding environment variables are not set.

### Deterministic Test Approach

The PLC runs a deterministic program (`tc3prg`) where all variable values are pure functions of a master cycle counter (`GVL_ProcessData.nMasterCycleCounter`). Tests:

1. Read the master counter and target variable in the same batch
2. Compute expected value from the counter using formula helpers
3. Allow a tolerance window of 5 counter ticks to account for read timing

This approach verifies correct type conversion and value delivery without relying on hardcoded expected values.

## Test Hardware

### TwinCAT 3: Beckhoff CX7000

| Property | Value |
|----------|-------|
| Model | CX7000-0004 |
| TwinCAT | 3.1 Build 4026 |
| Runtime Port | 851 |
| Task Cycle | 10 ms |

### TwinCAT 2: Beckhoff CX1020

| Property | Value |
|----------|-------|
| Model | CX1020-0111 |
| TwinCAT | 2.11 Build 2302 |
| Runtime Port | 801 |
| Task Cycle | 10 ms |

Both PLCs run the same deterministic test program (`tc3prg`) with a 10 ms task cycle. The master cycle counter increments every cycle.

## PLC Variable List (tc3prg)

All variables are deterministic functions of `GVL_ProcessData.nMasterCycleCounter`. Update formulas use `N = nMasterCycleCounter`.

### GVL_ProcessData (Global Variable List)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `nMasterCycleCounter` | ULINT | Increments every PLC cycle (10 ms) |
| `sStatusMessage` | STRING | `"{prefix}_{(N/100/26)%1000:03d}_{chr(A + (N/100)%26)}"` every 100 cycles |
| `eMachineState` | INT (enum) | `(N/100) % 6` → cycles through IDLE(0), STARTING(1), RUNNING(2), STOPPING(3), ERROR(4), MAINTENANCE(5) |
| `fGlobalReal` | REAL | Copy of `PRG_Machine.fbTempSensor.stReading.fValue` |
| `anCounters[0..4]` | ARRAY OF DINT | `anCounters[i] = DINT((N/100) * (i+1))` |
| `afMeasurements[0..2]` | ARRAY OF LREAL | `afMeasurements[i] = ((N/100) % ((i+1)*1000)) / ((i+1)*10)` |

#### GVL_ProcessData.stProductionStats (ST_ProductionStats)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `.nPartsProduced` | UDINT | `(N/100) * 2` |
| `.nPartsRejected` | UDINT | `(N/100) / 10` |
| `.fYieldPercent` | REAL | `100.0 - (rejected * 100.0 / produced)` |
| `.nBatchNumber` | UDINT | `(N/100) / 50` |

#### GVL_ProcessData.stMachineStatus.stMotor1 (ST_Motor)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `.fSpeed` | REAL | `(N/100) % 1500` |
| `.eState` | INT (enum) | `(N/100) % 6` |
| `.bEnabled` | BOOL | `eState IN (1, 2, 5)` |
| `.bError` | BOOL | `eState = 4` |
| `.fTorque` | REAL | `fSpeed * 0.1` |

#### GVL_ProcessData.stMachineStatus.stTemperature (ST_Sensor)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `.fValue` | REAL | `120.0 * ((N/100) % 1000) / 1000` |
| `.bValid` | BOOL | `((N/100) / 1000) % 2 = 0` |
| `.sUnit` | STRING | `"degC"` (constant) |

#### GVL_ProcessData.stMachineStatus.sMachineName

| Symbol | Type | Value |
|--------|------|-------|
| `sMachineName` | STRING | `"TestMachine_Line1"` (constant) |

### PRG_Diagnostics

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `nByte` | BYTE | `(N/100) % 256` |
| `nSint` | SINT | `SINT(N/100)` (wraps at 128 → -128) |
| `nInt` | INT | `INT((N/100) * 3)` |
| `nDint` | DINT | `DINT((N/100) * 7)` |
| `nWord` | WORD | `(N/100) % 65536` |
| `fReal` | REAL | `((N/100) % 1000) * 0.1` (sawtooth 0.0–99.9) |
| `bHeartbeat` | BOOL | `(N/50) % 2 = 1` (toggles every 50 cycles) |
| `nFastInt` | INT | Fast-updating variant |
| `nFastDint` | DINT | `DINT(N)` (updates every cycle) |
| `tTime` | TIME | Non-empty time value |
| `dDate` | DATE | Non-empty date value |
| `dtDateTime` | DATE_AND_TIME | Non-empty datetime value |
| `todTimeOfDay` | TOD | Non-empty time-of-day value |

#### PRG_Diagnostics.astSensorHistory[0] (ST_Sensor)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `.fValue` | REAL | `50.0 * ((N/20) % 100) / 100` |
| `.sUnit` | STRING | `"hist0"` (constant) |

### PRG_Machine.fbTempSensor.stReading (ST_Sensor)

| Symbol | Type | Update Formula |
|--------|------|----------------|
| `.fValue` | REAL | `120.0 * ((N/100) % 1000) / 1000` (same as stTemperature) |

## Dependencies

- [`RuneRoven/go-ads`](https://github.com/RuneRoven/go-ads) - ADS protocol library
- [`rs/zerolog`](https://github.com/rs/zerolog) - Logging (used by go-ads)
