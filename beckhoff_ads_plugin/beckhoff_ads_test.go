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

package beckhoff_ads_plugin_test

import (
	"context"
	"math"
	"os"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	adsLib "github.com/RuneRoven/go-ads"
	"github.com/redpanda-data/benthos/v4/public/service"

	ads "github.com/united-manufacturing-hub/benthos-umh/beckhoff_ads_plugin"
)

// adsHardwareTestConfig holds connection parameters for a Beckhoff PLC.
type adsHardwareTestConfig struct {
	targetIP    string
	targetAMS   string
	runtimePort int
}

// loadADSConfig loads PLC connection config from environment variables.
func loadADSConfig(ipEnv, amsEnv, portEnv string, defaultPort int) (adsHardwareTestConfig, bool) {
	targetIP := os.Getenv(ipEnv)
	targetAMS := os.Getenv(amsEnv)

	if targetIP == "" || targetAMS == "" {
		return adsHardwareTestConfig{}, false
	}

	runtimePort := defaultPort
	if portStr := os.Getenv(portEnv); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			runtimePort = p
		}
	}

	return adsHardwareTestConfig{
		targetIP:    targetIP,
		targetAMS:   targetAMS,
		runtimePort: runtimePort,
	}, true
}

// createTestInput builds an AdsCommInput configured for testing.
func createTestInput(cfg adsHardwareTestConfig, readType string, symbols []string) *ads.AdsCommInput {
	plcSymbols := ads.CreateSymbolList(symbols, 1000, 100, false)

	return &ads.AdsCommInput{
		TargetIP:         cfg.targetIP,
		TargetAMS:        cfg.targetAMS,
		TargetPort:       48898,
		RuntimePort:      cfg.runtimePort,
		HostAMS:          "auto",
		HostPort:         10500,
		ReadType:         readType,
		CycleTime:        1000,
		MaxDelay:         100,
		IntervalTime:     500 * time.Millisecond,
		RequestTimeout:   5 * time.Second,
		Symbols:          plcSymbols,
		NotificationChan: make(chan *adsLib.Update, 1000),
		TransmissionMode: adsLib.TransModeServerOnChange,
	}
}

// createTestInputWithCustomSymbols builds an input with pre-configured PlcSymbol list.
func createTestInputWithCustomSymbols(cfg adsHardwareTestConfig, readType string, symbols []ads.PlcSymbol) *ads.AdsCommInput {
	return &ads.AdsCommInput{
		TargetIP:         cfg.targetIP,
		TargetAMS:        cfg.targetAMS,
		TargetPort:       48898,
		RuntimePort:      cfg.runtimePort,
		HostAMS:          "auto",
		HostPort:         10500,
		ReadType:         readType,
		CycleTime:        1000,
		MaxDelay:         100,
		IntervalTime:     100 * time.Millisecond,
		RequestTimeout:   5 * time.Second,
		Symbols:          symbols,
		NotificationChan: make(chan *adsLib.Update, 1000),
		TransmissionMode: adsLib.TransModeServerCycle,
	}
}

// extractSymbolValue extracts a symbol's raw string value from a message batch.
func extractSymbolValue(batch service.MessageBatch, symbolName string) (string, bool) {
	// sanitize: replace dots and special chars with underscores (same logic as plugin)
	sanitized := ""
	for _, c := range symbolName {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			sanitized += string(c)
		} else {
			sanitized += "_"
		}
	}

	for _, msg := range batch {
		if name, ok := msg.MetaGet("symbol_name"); ok && name == sanitized {
			b, err := msg.AsBytes()
			if err != nil {
				return "", false
			}
			return string(b), true
		}
	}
	return "", false
}

func mustParseUint64(s string) uint64 {
	v, err := strconv.ParseUint(s, 10, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to parse %q as uint64", s)
	return v
}

func mustParseInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to parse %q as int64", s)
	return v
}

func mustParseFloat64(s string) float64 {
	v, err := strconv.ParseFloat(s, 64)
	ExpectWithOffset(1, err).NotTo(HaveOccurred(), "failed to parse %q as float64", s)
	return v
}

// verifyBoolWithTolerance checks a bool value against ExpectedBoolToggle within counter tolerance.
func verifyBoolWithTolerance(actual bool, masterCounter uint64, toggleCycles uint64) bool {
	for delta := uint64(0); delta <= 5; delta++ {
		if actual == ads.ExpectedBoolToggle(masterCounter+delta, toggleCycles) {
			return true
		}
	}
	return false
}

// describeADSHardwareTests defines the shared integration test suite for a Beckhoff PLC.
func describeADSHardwareTests(description string, ipEnv, amsEnv, portEnv string, defaultPort int) bool {
	return Describe(description, Ordered, func() {
		var (
			cfg    adsHardwareTestConfig
			ctx    context.Context
			cancel context.CancelFunc
		)

		BeforeAll(func() {
			var ok bool
			cfg, ok = loadADSConfig(ipEnv, amsEnv, portEnv, defaultPort)
			if !ok {
				Skip("Skipping: " + ipEnv + " / " + amsEnv + " not set")
			}
		})

		BeforeEach(func() {
			ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
		})

		AfterEach(func() {
			if cancel != nil {
				cancel()
			}
		})

		// ---- Config Parsing ----

		Context("Config Parsing", func() {
			It("should parse interval read config", func() {
				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				err := builder.SetYAML(`
input:
  ads:
    targetIP: "` + cfg.targetIP + `"
    targetAMS: "` + cfg.targetAMS + `"
    runtimePort: ` + strconv.Itoa(cfg.runtimePort) + `
    readType: interval
    intervalTime: 500
    symbols:
      - "PRG_Diagnostics.nInt"
      - "PRG_Diagnostics.fReal"
      - "PRG_Diagnostics.bHeartbeat"
output:
  drop: {}
`)
				Expect(err).NotTo(HaveOccurred())
				_ = builder
			})

			It("should parse notification config", func() {
				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				err := builder.SetYAML(`
input:
  ads:
    targetIP: "` + cfg.targetIP + `"
    targetAMS: "` + cfg.targetAMS + `"
    runtimePort: ` + strconv.Itoa(cfg.runtimePort) + `
    readType: notification
    cycleTime: 100
    maxDelay: 50
    symbols:
      - "PRG_Diagnostics.nFastInt"
output:
  drop: {}
`)
				Expect(err).NotTo(HaveOccurred())
				_ = builder
			})
		})

		// ---- Interval Read - Basic Types ----

		Context("Interval Read - Basic Types", func() {
			It("connects and reads scalar types", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.nInt",
					"PRG_Diagnostics.fReal",
					"PRG_Diagnostics.bHeartbeat",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batch).To(HaveLen(4))

				// Verify counter is > 0
				counterStr, found := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				Expect(found).To(BeTrue(), "nMasterCycleCounter not found in batch")
				counter := mustParseUint64(counterStr)
				Expect(counter).To(BeNumerically(">", 0))

				// Verify nInt parses
				nIntStr, found := extractSymbolValue(batch, "PRG_Diagnostics.nInt")
				Expect(found).To(BeTrue(), "nInt not found in batch")
				_ = mustParseInt64(nIntStr)

				// Verify fReal parses
				fRealStr, found := extractSymbolValue(batch, "PRG_Diagnostics.fReal")
				Expect(found).To(BeTrue(), "fReal not found in batch")
				_ = mustParseFloat64(fRealStr)

				// Verify bHeartbeat parses as bool
				boolStr, found := extractSymbolValue(batch, "PRG_Diagnostics.bHeartbeat")
				Expect(found).To(BeTrue(), "bHeartbeat not found in batch")
				_, err = strconv.ParseBool(boolStr)
				Expect(err).NotTo(HaveOccurred(), "bHeartbeat %q not parseable as bool", boolStr)
			})

			It("verifies deterministic values against master counter", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.nByte",
					"PRG_Diagnostics.nSint",
					"PRG_Diagnostics.nInt",
					"PRG_Diagnostics.nDint",
					"PRG_Diagnostics.nWord",
					"PRG_Diagnostics.fReal",
					"PRG_Diagnostics.bHeartbeat",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				counterStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(counterStr)

				// nByte: BYTE, updateCycles=100, step=1, 8-bit unsigned
				nByteStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nByte")
				nByte := mustParseUint64(nByteStr)
				Expect(ads.VerifyUintWithTolerance(nByte, mc, 100, 1, 8)).To(BeTrue(),
					"nByte=%d not deterministic for mc=%d", nByte, mc)

				// nSint: SINT, updateCycles=100, step=1, 8-bit signed
				nSintStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nSint")
				nSint := mustParseInt64(nSintStr)
				Expect(ads.VerifyIntWithTolerance(nSint, mc, 100, 1, 8)).To(BeTrue(),
					"nSint=%d not deterministic for mc=%d", nSint, mc)

				// nInt: INT, updateCycles=100, step=3, 16-bit signed
				nIntStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nInt")
				nInt := mustParseInt64(nIntStr)
				Expect(ads.VerifyIntWithTolerance(nInt, mc, 100, 3, 16)).To(BeTrue(),
					"nInt=%d not deterministic for mc=%d", nInt, mc)

				// nDint: DINT, updateCycles=100, step=7, 32-bit signed
				nDintStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nDint")
				nDint := mustParseInt64(nDintStr)
				Expect(ads.VerifyIntWithTolerance(nDint, mc, 100, 7, 32)).To(BeTrue(),
					"nDint=%d not deterministic for mc=%d", nDint, mc)

				// nWord: WORD, updateCycles=100, step=1, 16-bit unsigned
				nWordStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nWord")
				nWord := mustParseUint64(nWordStr)
				Expect(ads.VerifyUintWithTolerance(nWord, mc, 100, 1, 16)).To(BeTrue(),
					"nWord=%d not deterministic for mc=%d", nWord, mc)

				// fReal: sawtooth, updateCycles=100, mod=1000, mult=0.1
				fRealStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.fReal")
				fReal := mustParseFloat64(fRealStr)
				Expect(ads.VerifyFloatWithTolerance(fReal, mc, func(n uint64) float64 {
					return ads.ExpectedSawtoothFloat(n, 100, 1000, 0.1)
				}, 0.2)).To(BeTrue(),
					"fReal=%f not deterministic for mc=%d", fReal, mc)

				// bHeartbeat: toggle every 50 cycles
				boolStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.bHeartbeat")
				bVal, _ := strconv.ParseBool(boolStr)
				Expect(verifyBoolWithTolerance(bVal, mc, 50)).To(BeTrue(),
					"bHeartbeat=%v not deterministic for mc=%d", bVal, mc)
			})

			It("reads changing values over time", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.nFastDint",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch1, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				counter1Str, _ := extractSymbolValue(batch1, "GVL_ProcessData.nMasterCycleCounter")
				counter1 := mustParseUint64(counter1Str)

				time.Sleep(1 * time.Second)

				batch2, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				counter2Str, _ := extractSymbolValue(batch2, "GVL_ProcessData.nMasterCycleCounter")
				counter2 := mustParseUint64(counter2Str)

				Expect(counter2).To(BeNumerically(">", counter1),
					"master counter should increase over 1 second")
			})

			It("verifies value delta over time matches expected step rate", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.nInt",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch0, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				mc0Str, _ := extractSymbolValue(batch0, "GVL_ProcessData.nMasterCycleCounter")
				mc0 := mustParseUint64(mc0Str)
				nInt0Str, _ := extractSymbolValue(batch0, "PRG_Diagnostics.nInt")
				nInt0 := mustParseInt64(nInt0Str)

				time.Sleep(2 * time.Second)

				batch1, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				mc1Str, _ := extractSymbolValue(batch1, "GVL_ProcessData.nMasterCycleCounter")
				mc1 := mustParseUint64(mc1Str)
				nInt1Str, _ := extractSymbolValue(batch1, "PRG_Diagnostics.nInt")
				nInt1 := mustParseInt64(nInt1Str)

				// nInt: step=3, updateCycles=100
				cyclesElapsed := mc1 - mc0
				expectedDelta := int64(3) * int64(cyclesElapsed/100)
				actualDelta := nInt1 - nInt0

				Expect(math.Abs(float64(actualDelta-expectedDelta))).To(BeNumerically("<=", 6.0),
					"nInt delta: got %d, expected ~%d (mc0=%d, mc1=%d)",
					actualDelta, expectedDelta, mc0, mc1)
			})
		})

		// ---- Interval Read - Structs and Arrays ----

		Context("Interval Read - Structs and Arrays", func() {
			It("reads production stats struct", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.stProductionStats.nPartsProduced",
					"GVL_ProcessData.stProductionStats.nPartsRejected",
					"GVL_ProcessData.stProductionStats.fYieldPercent",
					"GVL_ProcessData.stProductionStats.nBatchNumber",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				// nPartsProduced = (mc/100) * 2
				ppStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stProductionStats.nPartsProduced")
				pp := mustParseUint64(ppStr)
				matched := false
				for delta := uint64(0); delta <= 5; delta++ {
					if pp == ads.ExpectedProductionPartsProduced(mc+delta) {
						matched = true
						break
					}
				}
				Expect(matched).To(BeTrue(), "nPartsProduced=%d not deterministic for mc=%d", pp, mc)

				// fYieldPercent
				yieldStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stProductionStats.fYieldPercent")
				yield := mustParseFloat64(yieldStr)
				Expect(ads.VerifyFloatWithTolerance(yield, mc, func(n uint64) float64 {
					return ads.ExpectedProductionYield(n)
				}, 0.1)).To(BeTrue(), "fYieldPercent=%f not deterministic for mc=%d", yield, mc)
			})

			It("reads motor struct fields", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.stMachineStatus.stMotor1.fSpeed",
					"GVL_ProcessData.stMachineStatus.stMotor1.eState",
					"GVL_ProcessData.stMachineStatus.stMotor1.bEnabled",
					"GVL_ProcessData.stMachineStatus.stMotor1.bError",
					"GVL_ProcessData.stMachineStatus.stMotor1.fTorque",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				speedStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stMachineStatus.stMotor1.fSpeed")
				speed := mustParseFloat64(speedStr)
				Expect(ads.VerifyFloatWithTolerance(speed, mc, func(n uint64) float64 {
					return ads.ExpectedMotorSpeed(n, 100)
				}, 1.0)).To(BeTrue(), "fSpeed=%f not deterministic for mc=%d", speed, mc)

				stateStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stMachineStatus.stMotor1.eState")
				state := mustParseInt64(stateStr)
				Expect(ads.VerifyIntWithTolerance(state, mc, 100, 1, 32)).To(BeFalse()) // state is not a counter
				// Instead verify it's in valid range and matches formula
				matched := false
				for delta := uint64(0); delta <= 5; delta++ {
					if state == ads.ExpectedMotorState(mc+delta, 100) {
						matched = true
						break
					}
				}
				Expect(matched).To(BeTrue(), "eState=%d not deterministic for mc=%d", state, mc)
			})

			It("reads sensor struct fields", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.stMachineStatus.stTemperature.fValue",
					"GVL_ProcessData.stMachineStatus.stTemperature.bValid",
					"GVL_ProcessData.stMachineStatus.stTemperature.sUnit",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				fValStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stMachineStatus.stTemperature.fValue")
				fVal := mustParseFloat64(fValStr)
				Expect(ads.VerifyFloatWithTolerance(fVal, mc, func(n uint64) float64 {
					return ads.ExpectedSensorValue(n, 100, 120.0, 1000)
				}, 0.5)).To(BeTrue(), "temperature fValue=%f not deterministic for mc=%d", fVal, mc)

				unitStr, _ := extractSymbolValue(batch, "GVL_ProcessData.stMachineStatus.stTemperature.sUnit")
				Expect(unitStr).To(Equal("degC"))
			})

			It("reads DINT array elements", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.anCounters[0]",
					"GVL_ProcessData.anCounters[1]",
					"GVL_ProcessData.anCounters[2]",
					"GVL_ProcessData.anCounters[3]",
					"GVL_ProcessData.anCounters[4]",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				for i := 0; i < 5; i++ {
					sym := "GVL_ProcessData.anCounters[" + strconv.Itoa(i) + "]"
					valStr, found := extractSymbolValue(batch, sym)
					Expect(found).To(BeTrue(), "array element %s not found", sym)
					val := mustParseInt64(valStr)

					matched := false
					for delta := uint64(0); delta <= 5; delta++ {
						if val == ads.ExpectedArrayCounter(mc+delta, i) {
							matched = true
							break
						}
					}
					Expect(matched).To(BeTrue(),
						"anCounters[%d]=%d not deterministic for mc=%d", i, val, mc)
				}
			})

			It("reads LREAL measurement array", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.afMeasurements[0]",
					"GVL_ProcessData.afMeasurements[1]",
					"GVL_ProcessData.afMeasurements[2]",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				for i := 0; i < 3; i++ {
					sym := "GVL_ProcessData.afMeasurements[" + strconv.Itoa(i) + "]"
					valStr, found := extractSymbolValue(batch, sym)
					Expect(found).To(BeTrue(), "array element %s not found", sym)
					val := mustParseFloat64(valStr)

					Expect(ads.VerifyFloatWithTolerance(val, mc, func(n uint64) float64 {
						return ads.ExpectedArrayMeasurement(n, i)
					}, 0.01)).To(BeTrue(),
						"afMeasurements[%d]=%f not deterministic for mc=%d", i, val, mc)
				}
			})

			It("reads sensor history array of structs", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.astSensorHistory[0].fValue",
					"PRG_Diagnostics.astSensorHistory[0].sUnit",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				fValStr, found := extractSymbolValue(batch, "PRG_Diagnostics.astSensorHistory[0].fValue")
				Expect(found).To(BeTrue())
				fVal := mustParseFloat64(fValStr)
				Expect(ads.VerifyFloatWithTolerance(fVal, mc, func(n uint64) float64 {
					return ads.ExpectedSensorValue(n, 20, 50.0, 100)
				}, 1.0)).To(BeTrue(),
					"astSensorHistory[0].fValue=%f not deterministic for mc=%d", fVal, mc)

				unitStr, found := extractSymbolValue(batch, "PRG_Diagnostics.astSensorHistory[0].sUnit")
				Expect(found).To(BeTrue())
				Expect(unitStr).To(Equal("hist0"))
			})
		})

		// ---- Interval Read - Strings and Enums ----

		Context("Interval Read - Strings and Enums", func() {
			It("reads string cycler", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.sStatusMessage",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				msgStr, found := extractSymbolValue(batch, "GVL_ProcessData.sStatusMessage")
				Expect(found).To(BeTrue())
				Expect(ads.VerifyStringWithTolerance(msgStr, mc, func(n uint64) string {
					return ads.ExpectedStringCycler(n, 100, "Machine")
				})).To(BeTrue(),
					"sStatusMessage=%q not deterministic for mc=%d", msgStr, mc)
			})

			It("reads machine state enum", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"GVL_ProcessData.eMachineState",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				mcStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
				mc := mustParseUint64(mcStr)

				stateStr, found := extractSymbolValue(batch, "GVL_ProcessData.eMachineState")
				Expect(found).To(BeTrue())
				state := mustParseInt64(stateStr)

				matched := false
				for delta := uint64(0); delta <= 5; delta++ {
					if state == ads.ExpectedMotorState(mc+delta, 100) {
						matched = true
						break
					}
				}
				Expect(matched).To(BeTrue(),
					"eMachineState=%d not deterministic for mc=%d", state, mc)
			})

			It("reads static machine name string", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.stMachineStatus.sMachineName",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				nameStr, found := extractSymbolValue(batch, "GVL_ProcessData.stMachineStatus.sMachineName")
				Expect(found).To(BeTrue())
				Expect(nameStr).To(Equal("TestMachine_Line1"))
			})

			It("reads time/date types as non-empty", func() {
				input := createTestInput(cfg, "interval", []string{
					"PRG_Diagnostics.tTime",
					"PRG_Diagnostics.dDate",
					"PRG_Diagnostics.dtDateTime",
					"PRG_Diagnostics.todTimeOfDay",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				for _, sym := range []string{"PRG_Diagnostics.tTime", "PRG_Diagnostics.dDate", "PRG_Diagnostics.dtDateTime", "PRG_Diagnostics.todTimeOfDay"} {
					val, found := extractSymbolValue(batch, sym)
					Expect(found).To(BeTrue(), "%s not found in batch", sym)
					Expect(val).NotTo(BeEmpty(), "%s should be non-empty", sym)
				}
			})
		})

		// ---- Interval Read - Poll Rate Verification ----

		Context("Interval Read - Poll Rate Verification", func() {
			It("fast poll collects multiple batches with values changing", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.nMasterCycleCounter",
					"PRG_Diagnostics.nFastDint",
				})
				input.IntervalTime = 200 * time.Millisecond
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var counters []uint64
				deadline := time.Now().Add(3 * time.Second)
				for time.Now().Before(deadline) {
					batch, _, readErr := input.ReadBatch(ctx)
					Expect(readErr).NotTo(HaveOccurred())
					cStr, _ := extractSymbolValue(batch, "GVL_ProcessData.nMasterCycleCounter")
					counters = append(counters, mustParseUint64(cStr))
				}

				Expect(len(counters)).To(BeNumerically(">=", 10),
					"expected >=10 batches in 3s at 200ms interval, got %d", len(counters))
				Expect(counters[len(counters)-1]).To(BeNumerically(">", counters[0]),
					"counter should increase over 3 seconds of polling")
			})

			It("slow variable shows few transitions at fast poll rate", func() {
				// nInt: updateCycles=100 (~1s). Poll at 100ms for 2s → expect ~2 transitions.
				input := createTestInput(cfg, "interval", []string{
					"PRG_Diagnostics.nInt",
				})
				input.IntervalTime = 100 * time.Millisecond
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var values []string
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) {
					batch, _, readErr := input.ReadBatch(ctx)
					Expect(readErr).NotTo(HaveOccurred())
					valStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nInt")
					values = append(values, valStr)
				}

				transitions := 0
				for i := 1; i < len(values); i++ {
					if values[i] != values[i-1] {
						transitions++
					}
				}
				Expect(transitions).To(BeNumerically(">=", 1),
					"nInt should change at least once in 2 seconds")
				Expect(transitions).To(BeNumerically("<=", 6),
					"nInt should not change more than ~6 times in 2 seconds, got %d", transitions)
			})

			It("fast variable shows distinct values at fast poll rate", func() {
				// nFastDint: updateCycles=1 (~10ms). Poll at 50ms for 1s.
				input := createTestInput(cfg, "interval", []string{
					"PRG_Diagnostics.nFastDint",
				})
				input.IntervalTime = 50 * time.Millisecond
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				seen := make(map[string]bool)
				deadline := time.Now().Add(1 * time.Second)
				for time.Now().Before(deadline) {
					batch, _, readErr := input.ReadBatch(ctx)
					Expect(readErr).NotTo(HaveOccurred())
					valStr, _ := extractSymbolValue(batch, "PRG_Diagnostics.nFastDint")
					seen[valStr] = true
				}

				Expect(len(seen)).To(BeNumerically(">", 1),
					"nFastDint should have multiple distinct values over 1s at 50ms poll")
			})
		})

		// ---- Notification Read ----

		Context("Notification Read", func() {
			It("receives notifications for a fast symbol", func() {
				// nFastDint: cycleTime=100ms, maxDelay=0, serverCycle
				symbols := []ads.PlcSymbol{
					{Name: "PRG_Diagnostics.nFastDint", MaxDelay: 0, CycleTime: 100},
				}
				input := createTestInputWithCustomSymbols(cfg, "notification", symbols)
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				var received int
				collectCtx, collectCancel := context.WithTimeout(ctx, 3*time.Second)
				defer collectCancel()

				for received < 50 {
					_, _, readErr := input.ReadBatch(collectCtx)
					if readErr != nil {
						break // timeout or context done
					}
					received++
				}

				Expect(received).To(BeNumerically(">=", 3),
					"expected at least 3 notifications over 3s at 100ms rate, got %d", received)
			})

			It("receives proportional notifications for multiple symbols at different rates", func() {
				symbols := []ads.PlcSymbol{
					{Name: "PRG_Diagnostics.nInt", MaxDelay: 100, CycleTime: 1000},    // slow: ~1/s
					{Name: "PRG_Diagnostics.nFastDint", MaxDelay: 0, CycleTime: 100},   // fast: ~10/s
				}
				input := createTestInputWithCustomSymbols(cfg, "notification", symbols)
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				countsBySymbol := make(map[string]int)
				collectCtx, collectCancel := context.WithTimeout(ctx, 5*time.Second)
				defer collectCancel()

				for {
					batch, _, readErr := input.ReadBatch(collectCtx)
					if readErr != nil {
						break
					}
					for _, msg := range batch {
						if name, ok := msg.MetaGet("symbol_name"); ok {
							countsBySymbol[name]++
						}
					}
				}

				// Both symbols should have received notifications
				Expect(countsBySymbol).To(HaveKey("PRG_Diagnostics_nInt"))
				Expect(countsBySymbol).To(HaveKey("PRG_Diagnostics_nFastDint"))

				// Fast should have more notifications than slow
				Expect(countsBySymbol["PRG_Diagnostics_nFastDint"]).To(
					BeNumerically(">", countsBySymbol["PRG_Diagnostics_nInt"]),
					"fast symbol should have more notifications than slow symbol")
			})
		})

		// ---- GVL Copy Consistency ----

		Context("GVL Copy Consistency", func() {
			It("GVL fGlobalReal approximately equals direct FB temperature read", func() {
				input := createTestInput(cfg, "interval", []string{
					"GVL_ProcessData.fGlobalReal",
					"PRG_Machine.fbTempSensor.stReading.fValue",
				})
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				gvlStr, found := extractSymbolValue(batch, "GVL_ProcessData.fGlobalReal")
				Expect(found).To(BeTrue())
				gvlVal := mustParseFloat64(gvlStr)

				directStr, found := extractSymbolValue(batch, "PRG_Machine.fbTempSensor.stReading.fValue")
				Expect(found).To(BeTrue())
				directVal := mustParseFloat64(directStr)

				// REAL precision + GVL copy lag: allow generous epsilon
				Expect(math.Abs(gvlVal - directVal)).To(BeNumerically("<=", 1.0),
					"fGlobalReal=%f should approximate fbTempSensor.fValue=%f", gvlVal, directVal)
			})
		})
	})
}

// TwinCAT 3: default runtime port 851
var _ = describeADSHardwareTests(
	"Beckhoff ADS Test Against TwinCAT 3 PLC (tc3prg)",
	"TEST_ADS_TC3_TARGET_IP", "TEST_ADS_TC3_TARGET_AMS", "TEST_ADS_TC3_RUNTIME_PORT",
	851,
)

// TwinCAT 2: default runtime port 801, same tc3prg symbols
var _ = describeADSHardwareTests(
	"Beckhoff ADS Test Against TwinCAT 2 PLC (tc3prg)",
	"TEST_ADS_TC2_TARGET_IP", "TEST_ADS_TC2_TARGET_AMS", "TEST_ADS_TC2_RUNTIME_PORT",
	801,
)
