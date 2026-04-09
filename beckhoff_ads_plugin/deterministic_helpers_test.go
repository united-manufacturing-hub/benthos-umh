// Copyright 2026 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beckhoff_ads_plugin

import (
	"fmt"
	"math"
)

// Deterministic formula helpers ported from tc3prg/test/helpers_test.go.
// These compute expected PLC values as pure functions of nMasterCycleCounter,
// allowing verification without a physical PLC.

// counterTolerance is the maximum number of cycles the master counter may advance
// between reading the counter and reading the target variable.
const counterTolerance = 5

// counterResetPeriod matches GVL_ProcessData.nCounterResetPeriod.
// All counters reset to 0 when nMasterCycleCounter reaches this value (~139 days at 10ms).
const counterResetPeriod = 1200000000

// ExpectedCounterValue computes (N / updateCycles) * stepSize, the raw ULINT counter value.
func ExpectedCounterValue(masterCounter uint64, updateCycles uint64, stepSize uint64) uint64 {
	if updateCycles == 0 {
		updateCycles = 1
	}
	return (masterCounter / updateCycles) * stepSize
}

// ExpectedIntValue computes expected counter value truncated to a signed type width.
func ExpectedIntValue(masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int) int64 {
	raw := ExpectedCounterValue(masterCounter, updateCycles, stepSize)
	return truncateToSigned(raw, bitWidth)
}

// ExpectedUintValue computes expected counter value truncated to an unsigned type width.
func ExpectedUintValue(masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int) uint64 {
	raw := ExpectedCounterValue(masterCounter, updateCycles, stepSize)
	return truncateToUnsigned(raw, bitWidth)
}

// ExpectedBoolToggle computes expected bool value for heartbeat pattern.
func ExpectedBoolToggle(masterCounter uint64, toggleCycles uint64) bool {
	toggles := masterCounter / toggleCycles
	return toggles%2 == 1
}

// ExpectedSawtoothFloat computes expected sawtooth REAL/LREAL value.
func ExpectedSawtoothFloat(masterCounter uint64, updateCycles uint64, counterMod uint64, multiplier float64) float64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	return float64(counterVal%counterMod) * multiplier
}

// ExpectedSensorValue computes expected FB_Sensor fValue.
func ExpectedSensorValue(masterCounter uint64, updateCycles uint64, fMaxValue float64, sawtoothSteps uint64) float64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	pos := counterVal % sawtoothSteps
	return fMaxValue * float64(pos) / float64(sawtoothSteps)
}

// ExpectedSensorValid computes expected FB_Sensor bValid.
func ExpectedSensorValid(masterCounter uint64, updateCycles uint64, sawtoothSteps uint64) bool {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	cycle := counterVal / sawtoothSteps
	return cycle%2 == 0
}

// ExpectedAlarmSeverity computes expected E_AlarmSeverity value (0-3).
func ExpectedAlarmSeverity(masterCounter uint64, updateCycles uint64, sawtoothSteps uint64) uint64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	cycle := counterVal / sawtoothSteps
	return cycle % 4
}

// ExpectedMotorSpeed computes expected FB_Motor stData.fSpeed.
func ExpectedMotorSpeed(masterCounter uint64, updateCycles uint64) float64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	return float64(counterVal % 1500)
}

// ExpectedMotorPosition computes expected FB_Motor stData.fPosition.
func ExpectedMotorPosition(masterCounter uint64, updateCycles uint64) float64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	return float64(counterVal % 10000)
}

// ExpectedMotorRevolutions computes expected FB_Motor stData.nRevolutions.
func ExpectedMotorRevolutions(masterCounter uint64, updateCycles uint64) uint64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	return counterVal / 10000
}

// ExpectedMotorState computes expected E_MachineState (0-5).
func ExpectedMotorState(masterCounter uint64, updateCycles uint64) int64 {
	counterVal := ExpectedCounterValue(masterCounter, updateCycles, 1)
	return int64(counterVal % 6)
}

// ExpectedMotorEnabled computes expected bEnabled.
func ExpectedMotorEnabled(masterCounter uint64, updateCycles uint64) bool {
	state := ExpectedMotorState(masterCounter, updateCycles)
	return state == 1 || state == 2 || state == 5 // STARTING, RUNNING, MAINTENANCE
}

// ExpectedMotorError computes expected bError.
func ExpectedMotorError(masterCounter uint64, updateCycles uint64) bool {
	state := ExpectedMotorState(masterCounter, updateCycles)
	return state == 4 // ERROR
}

// ExpectedMotorTorque computes expected fTorque.
func ExpectedMotorTorque(masterCounter uint64, updateCycles uint64) float64 {
	return ExpectedMotorSpeed(masterCounter, updateCycles) * 0.1
}

// ExpectedLwordROL computes expected LWORD after bit rotation.
func ExpectedLwordROL(masterCounter uint64, updateCycles uint64) uint64 {
	updates := masterCounter / updateCycles
	shift := updates % 64
	return rotateLeft64(1, shift)
}

// ExpectedStringCycler computes expected string pattern.
func ExpectedStringCycler(masterCounter uint64, updateCycles uint64, prefix string) string {
	totalUpdates := masterCounter / updateCycles
	letterIndex := totalUpdates % 26
	numericPart := (totalUpdates / 26) % 1000
	letter := string(rune('A' + letterIndex))
	return fmt.Sprintf("%s_%03d_%s", prefix, numericPart, letter)
}

// ExpectedGVLUpdates computes nUpdates in PRG_Machine.
func ExpectedGVLUpdates(masterCounter uint64) uint64 {
	return masterCounter / 100
}

// ExpectedProductionPartsProduced = nUpdates * 2
func ExpectedProductionPartsProduced(masterCounter uint64) uint64 {
	return ExpectedGVLUpdates(masterCounter) * 2
}

// ExpectedProductionPartsRejected = nUpdates / 10
func ExpectedProductionPartsRejected(masterCounter uint64) uint64 {
	return ExpectedGVLUpdates(masterCounter) / 10
}

// ExpectedProductionYield computes expected fYieldPercent.
func ExpectedProductionYield(masterCounter uint64) float64 {
	produced := ExpectedProductionPartsProduced(masterCounter)
	rejected := ExpectedProductionPartsRejected(masterCounter)
	if produced == 0 {
		return 100.0
	}
	return 100.0 - (float64(rejected) * 100.0 / float64(produced))
}

// ExpectedBatchNumber = nUpdates / 50
func ExpectedBatchNumber(masterCounter uint64) uint64 {
	return ExpectedGVLUpdates(masterCounter) / 50
}

// ExpectedArrayCounter computes expected GVL_ProcessData.anCounters[i].
func ExpectedArrayCounter(masterCounter uint64, index int) int64 {
	counterVal := ExpectedCounterValue(masterCounter, 100, uint64(index+1))
	return truncateToSigned(counterVal, 32)
}

// ExpectedArrayMeasurement computes expected GVL_ProcessData.afMeasurements[i].
func ExpectedArrayMeasurement(masterCounter uint64, index int) float64 {
	counterVal := ExpectedCounterValue(masterCounter, 100, 1)
	modVal := uint64((index + 1) * 1000)
	divisor := float64((index + 1) * 10)
	return float64(counterVal%modVal) / divisor
}

// ---- Verification with tolerance ----

// VerifyIntWithTolerance checks that actual matches the expected value for any
// master counter in [N, N+tolerance].
func VerifyIntWithTolerance(actual int64, masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int) bool {
	for delta := uint64(0); delta <= counterTolerance; delta++ {
		expected := ExpectedIntValue(masterCounter+delta, updateCycles, stepSize, bitWidth)
		if actual == expected {
			return true
		}
	}
	return false
}

// VerifyUintWithTolerance checks that actual matches for any counter in [N, N+tolerance].
func VerifyUintWithTolerance(actual uint64, masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int) bool {
	for delta := uint64(0); delta <= counterTolerance; delta++ {
		expected := ExpectedUintValue(masterCounter+delta, updateCycles, stepSize, bitWidth)
		if actual == expected {
			return true
		}
	}
	return false
}

// VerifyFloatWithTolerance checks float value with counter tolerance and float epsilon.
func VerifyFloatWithTolerance(actual float64, masterCounter uint64, calcExpected func(uint64) float64, epsilon float64) bool {
	for delta := uint64(0); delta <= counterTolerance; delta++ {
		expected := calcExpected(masterCounter + delta)
		if math.Abs(actual-expected) < epsilon {
			return true
		}
	}
	return false
}

// VerifyStringWithTolerance checks string value with counter tolerance.
func VerifyStringWithTolerance(actual string, masterCounter uint64, calcExpected func(uint64) string) bool {
	for delta := uint64(0); delta <= counterTolerance; delta++ {
		expected := calcExpected(masterCounter + delta)
		if actual == expected {
			return true
		}
	}
	return false
}

// ---- Internal utilities ----

func truncateToSigned(val uint64, bitWidth int) int64 {
	switch bitWidth {
	case 8:
		return int64(int8(val))
	case 16:
		return int64(int16(val))
	case 32:
		return int64(int32(val))
	case 64:
		return int64(val)
	default:
		panic(fmt.Sprintf("unsupported bitWidth: %d", bitWidth))
	}
}

func truncateToUnsigned(val uint64, bitWidth int) uint64 {
	switch bitWidth {
	case 8:
		return uint64(uint8(val))
	case 16:
		return uint64(uint16(val))
	case 32:
		return uint64(uint32(val))
	case 64:
		return val
	default:
		panic(fmt.Sprintf("unsupported bitWidth: %d", bitWidth))
	}
}

func rotateLeft64(val uint64, shift uint64) uint64 {
	shift = shift % 64
	return (val << shift) | (val >> (64 - shift))
}
