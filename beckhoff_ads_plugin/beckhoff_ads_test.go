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
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"

	ads "github.com/united-manufacturing-hub/benthos-umh/beckhoff_ads_plugin"
)

// plcTestSymbols holds all PLC symbol names used by integration tests.
// Each PLC type (TC2/TC3) has its own instance with correct naming conventions.
type plcTestSymbols struct {
	// Process data
	MasterCycleCounter string
	StatusMessage      string
	MachineState       string
	GlobalReal         string

	// Diagnostics (basic types)
	DiagNInt       string
	DiagFReal      string
	DiagBHeartbeat string
	DiagNByte      string
	DiagNSint      string
	DiagNDint      string
	DiagNWord      string
	DiagNFastDint  string

	// Time/date types
	DiagTime      string
	DiagDate      string
	DiagDateTime  string
	DiagTimeOfDay string

	// Production stats struct
	ProductionPartsProduced string
	ProductionPartsRejected string
	ProductionYieldPercent  string
	ProductionBatchNumber   string

	// Motor struct
	Motor1Speed   string
	Motor1State   string
	Motor1Enabled string
	Motor1Error   string
	Motor1Torque  string

	// Temperature sensor struct
	TempValue string
	TempValid string
	TempUnit  string

	// Machine name (static string)
	MachineName string

	// Arrays (indexed by caller)
	CounterFmt       string // fmt pattern, e.g. "GVL_ProcessData.anCounters[%d]"
	MeasurementFmt   string // fmt pattern, e.g. "GVL_ProcessData.afMeasurements[%d]"
	CounterCount     int
	MeasurementCount int

	// Sensor history array of structs
	SensorHistoryValueFmt string // e.g. "PRG_Diagnostics.astSensorHistory[%d].fValue"
	SensorHistoryUnitFmt  string

	// FB direct access (GVL copy consistency test)
	FBTempSensorValue string
}

// findSymbol searches a symbol list for one ending with the given suffix.
// Panics if not found — config is the source of truth, missing symbol = broken config.
func findSymbol(symbols []string, suffix string) string {
	for _, s := range symbols {
		if strings.HasSuffix(s, suffix) {
			return s
		}
	}
	panic(fmt.Sprintf("symbol with suffix %q not found in config", suffix))
}

// findArrayFmt finds array symbols matching a base name and returns a fmt pattern + count.
// E.g., for "anCounters" it finds "...anCounters[0]", "...anCounters[1]", etc.
// Returns the prefix + "anCounters[%d]" pattern and the count.
func findArrayFmt(symbols []string, arrayName string) (string, int) {
	count := 0
	fmtStr := ""
	for _, s := range symbols {
		if strings.Contains(s, arrayName+"[") {
			count++
			if fmtStr == "" {
				idx := strings.Index(s, arrayName+"[")
				fmtStr = s[:idx] + arrayName + "[%d]"
			}
		}
	}
	if fmtStr == "" {
		panic(fmt.Sprintf("array %q not found in config", arrayName))
	}
	return fmtStr, count
}

// findArrayStructFmt finds array-of-struct symbols and returns a fmt pattern.
// E.g., for ("astSensorHistory", ".fValue") finds "...astSensorHistory[0].fValue"
// and returns "...astSensorHistory[%d].fValue".
func findArrayStructFmt(symbols []string, arrayName string, fieldSuffix string) string {
	for _, s := range symbols {
		if strings.Contains(s, arrayName+"[") && strings.HasSuffix(s, fieldSuffix) {
			idx := strings.Index(s, arrayName+"[")
			return s[:idx] + arrayName + "[%d]" + fieldSuffix
		}
	}
	panic(fmt.Sprintf("array struct %q with field %q not found in config", arrayName, fieldSuffix))
}

// symbolsFromConfig builds plcTestSymbols from a parsed YAML config's symbol list.
// The config is the single source of truth for symbol naming.
func symbolsFromConfig(yamlConfig string) plcTestSymbols {
	symbols := extractSymbolsFromConfig(yamlConfig)

	counterFmt, counterCount := findArrayFmt(symbols, "anCounters")
	measFmt, measCount := findArrayFmt(symbols, "afMeasurements")

	return plcTestSymbols{
		MasterCycleCounter: findSymbol(symbols, "nMasterCycleCounter"),
		StatusMessage:      findSymbol(symbols, "sStatusMessage"),
		MachineState:       findSymbol(symbols, "eMachineState"),
		GlobalReal:         findSymbol(symbols, "fGlobalReal"),

		DiagNInt:       findSymbol(symbols, ".nInt"),
		DiagFReal:      findSymbol(symbols, ".fReal"),
		DiagBHeartbeat: findSymbol(symbols, ".bHeartbeat"),
		DiagNByte:      findSymbol(symbols, ".nByte"),
		DiagNSint:      findSymbol(symbols, ".nSint"),
		DiagNDint:      findSymbol(symbols, ".nDint"),
		DiagNWord:      findSymbol(symbols, ".nWord"),
		DiagNFastDint:  findSymbol(symbols, ".nFastDint"),

		DiagTime:      findSymbol(symbols, ".tTime"),
		DiagDate:      findSymbol(symbols, ".dDate"),
		DiagDateTime:  findSymbol(symbols, ".dtDateTime"),
		DiagTimeOfDay: findSymbol(symbols, ".todTimeOfDay"),

		ProductionPartsProduced: findSymbol(symbols, "stProductionStats.nPartsProduced"),
		ProductionPartsRejected: findSymbol(symbols, "stProductionStats.nPartsRejected"),
		ProductionYieldPercent:  findSymbol(symbols, "stProductionStats.fYieldPercent"),
		ProductionBatchNumber:   findSymbol(symbols, "stProductionStats.nBatchNumber"),

		Motor1Speed:   findSymbol(symbols, "stMotor1.fSpeed"),
		Motor1State:   findSymbol(symbols, "stMotor1.eState"),
		Motor1Enabled: findSymbol(symbols, "stMotor1.bEnabled"),
		Motor1Error:   findSymbol(symbols, "stMotor1.bError"),
		Motor1Torque:  findSymbol(symbols, "stMotor1.fTorque"),

		TempValue: findSymbol(symbols, "stTemperature.fValue"),
		TempValid: findSymbol(symbols, "stTemperature.bValid"),
		TempUnit:  findSymbol(symbols, "stTemperature.sUnit"),

		MachineName: findSymbol(symbols, "sMachineName"),

		CounterFmt:       counterFmt,
		MeasurementFmt:   measFmt,
		CounterCount:     counterCount,
		MeasurementCount: measCount,

		SensorHistoryValueFmt: findArrayStructFmt(symbols, "astSensorHistory", ".fValue"),
		SensorHistoryUnitFmt:  findArrayStructFmt(symbols, "astSensorHistory", ".sUnit"),

		FBTempSensorValue: findSymbol(symbols, "fbTempSensor.stReading.fValue"),
	}
}

// sanitizeSymbolName applies the same sanitization as the plugin (non-alnum → underscore).
func sanitizeSymbolName(s string) string {
	var b strings.Builder
	for _, c := range s {
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// adsHardwareTestConfig holds connection parameters for a Beckhoff PLC.
type adsHardwareTestConfig struct {
	targetIP      string
	targetAMS     string
	runtimePort   int
	routeUsername string
	routePassword string
	hostAMS       string // "auto" or explicit NetID; overrides derived value
	hostPort      int    // local AMS port; 0 → go-ads default/random port
}

// loadADSConfig loads PLC connection config from environment variables.
// ipEnv and amsEnv are required; all others are optional.
// HOST_AMS env var (derived from amsEnv prefix) sets a custom local AMS NetID,
// e.g. TEST_ADS_TC3_HOST_AMS=192.168.3.101.1.2 to avoid collisions when
// another ADS client from the same host is already connected.
// TEST_ADS_TC3_HOST_PORT sets the local AMS port (0 uses go-ads default/random port).
func loadADSConfig(ipEnv string, amsEnv string, portEnv string, defaultPort int, routeUserEnv string, routePassEnv string) (adsHardwareTestConfig, bool) {
	targetIP := os.Getenv(ipEnv)
	targetAMS := os.Getenv(amsEnv)

	if targetIP == "" || targetAMS == "" {
		return adsHardwareTestConfig{}, false
	}

	runtimePort := defaultPort
	if portStr := os.Getenv(portEnv); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			Fail(fmt.Sprintf("invalid %s value %q: %v", portEnv, portStr, err))
			return adsHardwareTestConfig{}, false
		}
		runtimePort = p
	}

	// Derive HOST_AMS / HOST_PORT env names from the amsEnv prefix.
	// e.g. TEST_ADS_TC3_TARGET_AMS → TEST_ADS_TC3_HOST_AMS / TEST_ADS_TC3_HOST_PORT
	hostAMSEnv := strings.Replace(amsEnv, "TARGET_AMS", "HOST_AMS", 1)
	hostPortEnv := strings.Replace(amsEnv, "TARGET_AMS", "HOST_PORT", 1)
	hostAMS := os.Getenv(hostAMSEnv)
	if hostAMS == "" {
		hostAMS = "auto"
	}
	hostPort := 0 // 0 = random port (go-ads default)
	if portStr := os.Getenv(hostPortEnv); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			Fail(fmt.Sprintf("invalid %s value %q: %v", hostPortEnv, portStr, err))
			return adsHardwareTestConfig{}, false
		}
		hostPort = p
	}

	return adsHardwareTestConfig{
		targetIP:      targetIP,
		targetAMS:     targetAMS,
		runtimePort:   runtimePort,
		routeUsername: os.Getenv(routeUserEnv),
		routePassword: os.Getenv(routePassEnv),
		hostAMS:       hostAMS,
		hostPort:      hostPort,
	}, true
}

// testLogger returns a Benthos logger suitable for use in hardware tests.
func testLogger() *service.Logger {
	return service.MockResources().Logger()
}

// createTestInput builds an AdsCommInput configured for testing.
func createTestInput(cfg adsHardwareTestConfig, readType string, symbols []string) *ads.AdsCommInput {
	plcSymbols, warnings := ads.CreateSymbolList(symbols, 1000*time.Millisecond, 100*time.Millisecond)
	ExpectWithOffset(1, warnings).To(BeEmpty())

	return &ads.AdsCommInput{
		TargetIP:         cfg.targetIP,
		TargetAMS:        cfg.targetAMS,
		TargetPort:       48898,
		RuntimePort:      cfg.runtimePort,
		HostAMS:          cfg.hostAMS,
		HostPort:         cfg.hostPort,
		ReadType:         readType,
		CycleTime:        1000 * time.Millisecond,
		MaxDelay:         100 * time.Millisecond,
		IntervalTime:     500 * time.Millisecond,
		RequestTimeout:   5 * time.Second,
		Symbols:          plcSymbols,
		NotificationChan: make(chan *adsLib.Update, 1000),
		TransmissionMode: adsLib.TransModeServerOnChange,
		Log:              testLogger(),
	}
}

// createTestInputWithCustomSymbols builds an input with pre-configured PlcSymbol list.
func createTestInputWithCustomSymbols(cfg adsHardwareTestConfig, readType string, symbols []ads.PlcSymbol) *ads.AdsCommInput {
	return &ads.AdsCommInput{
		TargetIP:         cfg.targetIP,
		TargetAMS:        cfg.targetAMS,
		TargetPort:       48898,
		RuntimePort:      cfg.runtimePort,
		HostAMS:          cfg.hostAMS,
		HostPort:         cfg.hostPort,
		ReadType:         readType,
		CycleTime:        1000 * time.Millisecond,
		MaxDelay:         100 * time.Millisecond,
		IntervalTime:     100 * time.Millisecond,
		RequestTimeout:   5 * time.Second,
		Symbols:          symbols,
		NotificationChan: make(chan *adsLib.Update, 1000),
		TransmissionMode: adsLib.TransModeServerCycle,
		Log:              testLogger(),
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
// Tolerance of 150 matches counterTolerance in deterministic_helpers_test.go.
func verifyBoolWithTolerance(actual bool, masterCounter uint64, toggleCycles uint64) bool {
	const tolerance = uint64(150)
	for delta := uint64(0); delta <= tolerance; delta++ {
		if actual == ads.ExpectedBoolToggle(masterCounter+delta, toggleCycles) {
			return true
		}
		if delta > 0 && masterCounter >= delta {
			if actual == ads.ExpectedBoolToggle(masterCounter-delta, toggleCycles) {
				return true
			}
		}
	}
	return false
}

// describeADSHardwareTests defines the shared integration test suite for a Beckhoff PLC.
// configFile is the YAML config filename in testdata/ — single source of truth for symbols.
func describeADSHardwareTests(description string, ipEnv string, amsEnv string, portEnv string, defaultPort int, routeUserEnv string, routePassEnv string, configFile string) bool {
	return Describe(description, Ordered, func() {
		var (
			cfg         adsHardwareTestConfig
			syms        plcTestSymbols
			allSymbols  []string          // stored so shared session can be created after route reg
			sharedInput *ads.AdsCommInput // single long-lived interval connection, created after route registration
			ctx         context.Context
			cancel      context.CancelFunc
		)

		BeforeAll(func() {
			var ok bool
			cfg, ok = loadADSConfig(ipEnv, amsEnv, portEnv, defaultPort, routeUserEnv, routePassEnv)
			if !ok {
				Skip("Skipping: " + ipEnv + " / " + amsEnv + " not set")
			}
			yamlCfg := loadTestConfig(configFile)
			syms = symbolsFromConfig(yamlCfg)
			allSymbols = extractSymbolsFromConfig(yamlCfg)
			// sharedInput is NOT created here — route registration disrupts the ADS layer,
			// so we create it after route reg completes (see "Shared Session Setup" below).
		})

		AfterAll(func() {
			if sharedInput != nil {
				sharedInput.Close(context.Background())
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

		// readSharedBatch returns one batch from sharedInput, retrying until data arrives
		// or the test context expires. Use for tests that only need a single snapshot.
		// Timeout sized to absorb go-ads cross-cycle flap-backoff (1s/5s/15s tiers)
		// while sharedInput's cache is re-resolved after a reconnect — empty batches
		// are expected during that window.
		readSharedBatch := func() service.MessageBatch {
			var batch service.MessageBatch
			Eventually(func() bool {
				b, _, e := sharedInput.ReadBatch(ctx)
				if errors.Is(e, service.ErrNotConnected) {
					_ = sharedInput.Connect(ctx)
					return false
				}
				if e != nil || len(b) == 0 {
					return false
				}
				batch = b
				return true
			}, 30*time.Second, 500*time.Millisecond).Should(BeTrue(), "shared input failed to produce batch")
			return batch
		}

		// ---- Route Registration ----
		// Must run first (Ordered suite). Registers route via plugin's Connect()
		// with credentials, disconnects, then verifies a credential-free connection
		// works. All subsequent tests connect without route credentials to prove
		// route persistence.

		Context("Route Registration", func() {
			It("registers route and connects without credentials", func() {
				if cfg.routeUsername == "" || cfg.routePassword == "" {
					Skip("route credentials not provided, skipping route registration test")
				}

				// Step 1: Connect WITH credentials — plugin registers route internally.
				syms0, _ := ads.CreateSymbolList([]string{syms.MasterCycleCounter}, 1000*time.Millisecond, 100*time.Millisecond)
				inputWithRoute := &ads.AdsCommInput{
					TargetIP:         cfg.targetIP,
					TargetAMS:        cfg.targetAMS,
					TargetPort:       48898,
					RuntimePort:      cfg.runtimePort,
					HostAMS:          cfg.hostAMS,
					HostPort:         cfg.hostPort,
					ReadType:         "interval",
					CycleTime:        1000 * time.Millisecond,
					MaxDelay:         100 * time.Millisecond,
					IntervalTime:     500 * time.Millisecond,
					RequestTimeout:   5 * time.Second,
					Symbols:          syms0,
					NotificationChan: make(chan *adsLib.Update, 1000),
					TransmissionMode: adsLib.TransModeServerOnChange,
					Username:         cfg.routeUsername,
					Password:         cfg.routePassword,
				}

				// Retry connect — first TCP dial can fail transiently (EHOSTUNREACH /
				// ARP miss) even when the PLC is reachable. go-ads rolls FSM back to
				// Disconnected on Connect failure so the same session can be retried.
				var err error
				Eventually(func() error {
					err = inputWithRoute.Connect(ctx)
					return err
				}, 30*time.Second, 2*time.Second).Should(Succeed(),
					"connection with route credentials failed: %v", err)
				// Step 2 proves data flows after the route-registration reconnect.
				inputWithRoute.Close(ctx)

				// Step 2: Connect WITHOUT credentials — proves route was registered.
				input := createTestInput(cfg, "interval", []string{syms.MasterCycleCounter})
				defer input.Close(ctx)

				err = input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred(),
					"connection without credentials failed — route registration did not persist")

				// PLC ADS may need a moment to settle after route-registration reconnect.
				// ReadBatch returns ErrNotConnected on transient failures; reconnect and retry.
				var batch service.MessageBatch
				Eventually(func() bool {
					b, _, e := input.ReadBatch(ctx)
					if errors.Is(e, service.ErrNotConnected) {
						_ = input.Connect(ctx)
						return false
					}
					if e != nil || len(b) == 0 {
						return false
					}
					batch = b
					return true
				}, 5*time.Second, 200*time.Millisecond).Should(BeTrue(),
					"timed out waiting for first batch after route registration")

				counterStr, found := extractSymbolValue(batch, syms.MasterCycleCounter)
				Expect(found).To(BeTrue())
				counter := mustParseUint64(counterStr)
				Expect(counter).To(BeNumerically(">", 0),
					"master counter should be > 0 after credential-free connection")
			})
		})

		// ---- Shared Session Setup ----
		// Runs after route registration (Ordered). Creates the long-lived shared interval
		// session used by all subsequent interval tests. Placing it here ensures route reg
		// has already settled before the session is established.

		Context("Shared Session Setup", func() {
			It("establishes shared interval session", func() {
				sharedInput = createTestInput(cfg, "interval", allSymbols)
				err := sharedInput.Connect(ctx)
				Expect(err).NotTo(HaveOccurred(), "shared input Connect failed")
				// Warm up: discard first batch so subsequent tests start with a stable session.
				_ = readSharedBatch()
			})
		})

		// ---- Config Parsing ----

		Context("Config Parsing", func() {
			It("should parse interval read config", func() {
				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				err := builder.AddInputYAML(`
ads:
  targetIP: "` + cfg.targetIP + `"
  targetAMS: "` + cfg.targetAMS + `"
  runtimePort: ` + strconv.Itoa(cfg.runtimePort) + `
  readType: interval
  intervalTime: 500ms
  symbols:
    - "` + syms.DiagNInt + `"
    - "` + syms.DiagFReal + `"
    - "` + syms.DiagBHeartbeat + `"
`)
				Expect(err).NotTo(HaveOccurred())
				_ = builder
			})

			It("should parse notification config", func() {
				env := service.NewEnvironment()
				builder := env.NewStreamBuilder()
				err := builder.AddInputYAML(`
ads:
  targetIP: "` + cfg.targetIP + `"
  targetAMS: "` + cfg.targetAMS + `"
  runtimePort: ` + strconv.Itoa(cfg.runtimePort) + `
  readType: notification
  cycleTime: 100ms
  maxDelay: 50ms
  symbols:
    - "` + syms.DiagNFastDint + `"
`)
				Expect(err).NotTo(HaveOccurred())
				_ = builder
			})
		})

		// ---- Interval Read - Basic Types ----
		// These tests use the shared session — no connect/disconnect overhead.

		Context("Interval Read - Basic Types", func() {
			It("reads scalar types", func() {
				batch := readSharedBatch()

				counterStr, found := extractSymbolValue(batch, syms.MasterCycleCounter)
				Expect(found).To(BeTrue(), "nMasterCycleCounter not found in batch")
				counter := mustParseUint64(counterStr)
				Expect(counter).To(BeNumerically(">", 0))

				nIntStr, found := extractSymbolValue(batch, syms.DiagNInt)
				Expect(found).To(BeTrue(), "nInt not found in batch")
				_ = mustParseInt64(nIntStr)

				fRealStr, found := extractSymbolValue(batch, syms.DiagFReal)
				Expect(found).To(BeTrue(), "fReal not found in batch")
				_ = mustParseFloat64(fRealStr)

				boolStr, found := extractSymbolValue(batch, syms.DiagBHeartbeat)
				Expect(found).To(BeTrue(), "bHeartbeat not found in batch")
				_, err := strconv.ParseBool(boolStr)
				Expect(err).NotTo(HaveOccurred(), "bHeartbeat %q not parseable as bool", boolStr)
			})

			It("verifies deterministic values against master counter", func() {
				batch := readSharedBatch()

				counterStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(counterStr)

				nByteStr, _ := extractSymbolValue(batch, syms.DiagNByte)
				nByte := mustParseUint64(nByteStr)
				Expect(ads.VerifyUintWithTolerance(nByte, mc, 100, 1, 8)).To(BeTrue(),
					"nByte=%d not deterministic for mc=%d", nByte, mc)

				nSintStr, _ := extractSymbolValue(batch, syms.DiagNSint)
				nSint := mustParseInt64(nSintStr)
				Expect(ads.VerifyIntWithTolerance(nSint, mc, 100, 1, 8)).To(BeTrue(),
					"nSint=%d not deterministic for mc=%d", nSint, mc)

				nIntStr, _ := extractSymbolValue(batch, syms.DiagNInt)
				nInt := mustParseInt64(nIntStr)
				Expect(ads.VerifyIntWithTolerance(nInt, mc, 100, 3, 16)).To(BeTrue(),
					"nInt=%d not deterministic for mc=%d", nInt, mc)

				nDintStr, _ := extractSymbolValue(batch, syms.DiagNDint)
				nDint := mustParseInt64(nDintStr)
				Expect(ads.VerifyIntWithTolerance(nDint, mc, 100, 7, 32)).To(BeTrue(),
					"nDint=%d not deterministic for mc=%d", nDint, mc)

				nWordStr, _ := extractSymbolValue(batch, syms.DiagNWord)
				nWord := mustParseUint64(nWordStr)
				Expect(ads.VerifyUintWithTolerance(nWord, mc, 100, 1, 16)).To(BeTrue(),
					"nWord=%d not deterministic for mc=%d", nWord, mc)

				fRealStr, _ := extractSymbolValue(batch, syms.DiagFReal)
				fReal := mustParseFloat64(fRealStr)
				Expect(ads.VerifyFloatWithTolerance(fReal, mc, func(n uint64) float64 {
					return ads.ExpectedSawtoothFloat(n, 100, 1000, 0.1)
				}, 0.2)).To(BeTrue(),
					"fReal=%f not deterministic for mc=%d", fReal, mc)

				boolStr, _ := extractSymbolValue(batch, syms.DiagBHeartbeat)
				bVal, _ := strconv.ParseBool(boolStr)
				Expect(verifyBoolWithTolerance(bVal, mc, 50)).To(BeTrue(),
					"bHeartbeat=%v not deterministic for mc=%d", bVal, mc)
			})

			It("reads changing values over time", func() {
				batch1 := readSharedBatch()
				counter1Str, _ := extractSymbolValue(batch1, syms.MasterCycleCounter)
				counter1 := mustParseUint64(counter1Str)

				time.Sleep(1 * time.Second)

				batch2 := readSharedBatch()
				counter2Str, _ := extractSymbolValue(batch2, syms.MasterCycleCounter)
				counter2 := mustParseUint64(counter2Str)

				Expect(counter2).To(BeNumerically(">", counter1),
					"master counter should increase over 1 second")
			})

			It("verifies value delta over time matches expected step rate", func() {
				batch0 := readSharedBatch()
				mc0Str, _ := extractSymbolValue(batch0, syms.MasterCycleCounter)
				mc0 := mustParseUint64(mc0Str)
				nInt0Str, _ := extractSymbolValue(batch0, syms.DiagNInt)
				nInt0 := mustParseInt64(nInt0Str)

				time.Sleep(2 * time.Second)

				batch1 := readSharedBatch()
				mc1Str, _ := extractSymbolValue(batch1, syms.MasterCycleCounter)
				mc1 := mustParseUint64(mc1Str)
				nInt1Str, _ := extractSymbolValue(batch1, syms.DiagNInt)
				nInt1 := mustParseInt64(nInt1Str)

				Expect(mc1).To(BeNumerically(">=", mc0),
					"master counter moved backwards/reset (mc0=%d, mc1=%d)", mc0, mc1)
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
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				ppStr, _ := extractSymbolValue(batch, syms.ProductionPartsProduced)
				pp := mustParseUint64(ppStr)
				matched := false
				for delta := uint64(0); delta <= 5; delta++ {
					if pp == ads.ExpectedProductionPartsProduced(mc+delta) {
						matched = true
						break
					}
				}
				Expect(matched).To(BeTrue(), "nPartsProduced=%d not deterministic for mc=%d", pp, mc)

				yieldStr, _ := extractSymbolValue(batch, syms.ProductionYieldPercent)
				yield := mustParseFloat64(yieldStr)
				Expect(ads.VerifyFloatWithTolerance(yield, mc, func(n uint64) float64 {
					return ads.ExpectedProductionYield(n)
				}, 0.1)).To(BeTrue(), "fYieldPercent=%f not deterministic for mc=%d", yield, mc)
			})

			It("reads motor struct fields", func() {
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				speedStr, _ := extractSymbolValue(batch, syms.Motor1Speed)
				speed := mustParseFloat64(speedStr)
				Expect(ads.VerifyFloatWithTolerance(speed, mc, func(n uint64) float64 {
					return ads.ExpectedMotorSpeed(n, 100)
				}, 1.0)).To(BeTrue(), "fSpeed=%f not deterministic for mc=%d", speed, mc)

				stateStr, _ := extractSymbolValue(batch, syms.Motor1State)
				state := mustParseInt64(stateStr)
				Expect(ads.VerifyIntWithTolerance(state, mc, 100, 1, 32)).To(BeFalse()) // state is not a counter
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
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				fValStr, _ := extractSymbolValue(batch, syms.TempValue)
				fVal := mustParseFloat64(fValStr)
				Expect(ads.VerifyFloatWithTolerance(fVal, mc, func(n uint64) float64 {
					return ads.ExpectedSensorValue(n, 100, 120.0, 1000)
				}, 0.5)).To(BeTrue(), "temperature fValue=%f not deterministic for mc=%d", fVal, mc)

				unitStr, _ := extractSymbolValue(batch, syms.TempUnit)
				Expect(unitStr).To(Equal("degC"))
			})

			It("reads DINT array elements", func() {
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				for i := 0; i < syms.CounterCount; i++ {
					sym := fmt.Sprintf(syms.CounterFmt, i)
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
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				for i := 0; i < syms.MeasurementCount; i++ {
					sym := fmt.Sprintf(syms.MeasurementFmt, i)
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
				sensorVal := fmt.Sprintf(syms.SensorHistoryValueFmt, 0)
				sensorUnit := fmt.Sprintf(syms.SensorHistoryUnitFmt, 0)

				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				fValStr, found := extractSymbolValue(batch, sensorVal)
				Expect(found).To(BeTrue())
				fVal := mustParseFloat64(fValStr)
				Expect(ads.VerifyFloatWithTolerance(fVal, mc, func(n uint64) float64 {
					return ads.ExpectedSensorValue(n, 20, 50.0, 100)
				}, 1.0)).To(BeTrue(),
					"astSensorHistory[0].fValue=%f not deterministic for mc=%d", fVal, mc)

				unitStr, found := extractSymbolValue(batch, sensorUnit)
				Expect(found).To(BeTrue())
				Expect(unitStr).To(Equal("hist0"))
			})
		})

		// ---- Interval Read - Strings and Enums ----

		Context("Interval Read - Strings and Enums", func() {
			It("reads string cycler", func() {
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				msgStr, found := extractSymbolValue(batch, syms.StatusMessage)
				Expect(found).To(BeTrue())
				Expect(ads.VerifyStringWithTolerance(msgStr, mc, func(n uint64) string {
					return ads.ExpectedStringCycler(n, 100, "Machine")
				})).To(BeTrue(),
					"sStatusMessage=%q not deterministic for mc=%d", msgStr, mc)
			})

			It("reads machine state enum", func() {
				batch := readSharedBatch()

				mcStr, _ := extractSymbolValue(batch, syms.MasterCycleCounter)
				mc := mustParseUint64(mcStr)

				stateStr, found := extractSymbolValue(batch, syms.MachineState)
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
				batch := readSharedBatch()

				nameStr, found := extractSymbolValue(batch, syms.MachineName)
				Expect(found).To(BeTrue())
				Expect(nameStr).To(Equal("TestMachine_Line1"))
			})

			It("reads time/date types as non-empty", func() {
				batch := readSharedBatch()

				for _, sym := range []string{syms.DiagTime, syms.DiagDate, syms.DiagDateTime, syms.DiagTimeOfDay} {
					val, found := extractSymbolValue(batch, sym)
					Expect(found).To(BeTrue(), "%s not found in batch", sym)
					Expect(val).NotTo(BeEmpty(), "%s should be non-empty", sym)
				}
			})
		})

		// ---- Interval Read - Poll Rate Verification ----
		// These tests reuse sharedInput. Fresh per-It sessions used to hammer the
		// PLC route table on full suite runs (every Connect=route probe=stale
		// conntrack entry on PLC), so we share. Assertions are cadence-agnostic:
		// they verify behavior (counter monotonic, slow var stable, fast var
		// varies) rather than exact batch counts at a configured IntervalTime.

		Context("Interval Read - Poll Rate Verification", func() {
			It("counter increases over a sampling window", func() {
				var counters []uint64
				// Window sized for TC2's slower SumReadEx (0xF083) fallback
				// over ~40 symbols on sharedInput plus any in-flight stale
				// notification noise from prior contexts. TC3 (SumReadEx2)
				// finishes well within this window.
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					batch := readSharedBatch()
					cStr, ok := extractSymbolValue(batch, syms.MasterCycleCounter)
					if !ok || cStr == "" {
						continue
					}
					counters = append(counters, mustParseUint64(cStr))
				}

				Expect(len(counters)).To(BeNumerically(">=", 2),
					"expected at least 2 batches in 10s, got %d", len(counters))
				Expect(counters[len(counters)-1]).To(BeNumerically(">", counters[0]),
					"counter should increase over the sampling window")
			})

			It("slow variable shows bounded transitions", func() {
				var values []string
				// Window sized for TC2's slower SumReadEx (0xF083) fallback
				// over ~40 symbols on sharedInput plus any in-flight stale
				// notification noise from prior contexts. TC3 (SumReadEx2)
				// finishes well within this window.
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					batch := readSharedBatch()
					valStr, ok := extractSymbolValue(batch, syms.DiagNInt)
					if !ok || valStr == "" {
						continue
					}
					values = append(values, valStr)
				}

				Expect(len(values)).To(BeNumerically(">=", 2),
					"expected at least 2 samples in 3s, got %d", len(values))
				transitions := 0
				for i := 1; i < len(values); i++ {
					if values[i] != values[i-1] {
						transitions++
					}
				}
				// Slow variable should change at some point but not on every sample.
				Expect(transitions).To(BeNumerically(">=", 1),
					"nInt should change at least once in sampling window")
				Expect(transitions).To(BeNumerically("<", len(values)),
					"nInt (slow variable) should not change on every sample")
			})

			It("fast variable shows multiple distinct values", func() {
				seen := make(map[string]bool)
				deadline := time.Now().Add(5 * time.Second)
				for time.Now().Before(deadline) {
					batch := readSharedBatch()
					valStr, ok := extractSymbolValue(batch, syms.DiagNFastDint)
					if !ok || valStr == "" {
						continue
					}
					seen[valStr] = true
				}

				Expect(len(seen)).To(BeNumerically(">", 1),
					"nFastDint should have multiple distinct values over 5s")
			})
		})

		// ---- GVL Copy Consistency ----
		// Runs while sharedInput is still warm and the PLC route table is unstressed.
		// Must precede Notification Read, which creates many fresh sessions and
		// drives the PLC into flap territory (see Notification Read NOTE).

		Context("GVL Copy Consistency", func() {
			It("fGlobalReal approximately equals direct FB temperature read", func() {
				batch := readSharedBatch()

				gvlStr, found := extractSymbolValue(batch, syms.GlobalReal)
				Expect(found).To(BeTrue())
				gvlVal := mustParseFloat64(gvlStr)

				directStr, found := extractSymbolValue(batch, syms.FBTempSensorValue)
				Expect(found).To(BeTrue())
				directVal := mustParseFloat64(directStr)

				Expect(math.Abs(gvlVal-directVal)).To(BeNumerically("<=", 1.0),
					"fGlobalReal=%f should approximate fbTempSensor.fValue=%f", gvlVal, directVal)
			})
		})

		// ---- Notification Read ----
		// Placed last because each It creates a fresh session that the PLC may
		// briefly reject (RST after route probe) under load, triggering flap
		// backoff. Running after every sharedInput-based test means those don't
		// suffer collateral damage from this Context's churn.

		Context("Notification Read", func() {
			// Insert a small delay between specs so the PLC's route-table /
			// connection-tracking state has a moment to recover between fresh
			// sessions. Without this the Nth fresh Connect arrives while the PLC
			// is still cleaning up from the (N-1)th, increasing flap rate.
			AfterEach(func() {
				time.Sleep(2 * time.Second)
			})

			// NOTE: On TC3, "received notification for unknown handle" warnings are expected
			// during rapid test connection cycling. Each test creates a new connection sharing
			// the same AMS NetID. The PLC may still deliver notifications from the previous
			// test's subscriptions to the new connection before cleaning them up. This only
			// occurs in test suites — production uses a single long-lived connection where
			// stale cross-connection notifications do not happen.

			// collectNotifications subscribes to symbols via notification mode and collects
			// values for the given duration. Returns map[sanitizedName][]string (values in order).
			collectNotifications := func(symbols []ads.PlcSymbol, duration time.Duration) map[string][]string {
				input := createTestInputWithCustomSymbols(cfg, "notification", symbols)
				defer input.Close(ctx)

				err := input.Connect(ctx)
				Expect(err).NotTo(HaveOccurred())

				valuesBySymbol := make(map[string][]string)
				collectCtx, collectCancel := context.WithTimeout(ctx, duration)
				defer collectCancel()

				for collectCtx.Err() == nil {
					batch, _, readErr := input.ReadBatch(collectCtx)
					if readErr != nil {
						if collectCtx.Err() == nil {
							Expect(readErr).NotTo(HaveOccurred())
						}
						break
					}
					for _, msg := range batch {
						if name, ok := msg.MetaGet("symbol_name"); ok {
							b, err := msg.AsBytes()
							Expect(err).NotTo(HaveOccurred())
							valuesBySymbol[name] = append(valuesBySymbol[name], string(b))
						}
					}
				}
				return valuesBySymbol
			}

			It("first ReadBatch after Connect contains initial sample for all subscribed symbols", func() {
				symbols := []ads.PlcSymbol{
					{Name: syms.MasterCycleCounter, CycleTime: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
					{Name: syms.DiagNInt, CycleTime: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
					{Name: syms.DiagFReal, CycleTime: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
				}
				input := createTestInputWithCustomSymbols(cfg, "notification", symbols)
				defer input.Close(ctx)

				Expect(input.Connect(ctx)).NotTo(HaveOccurred())

				// Connect() waits for initial samples from all registered symbols before
				// returning. First ReadBatch must therefore contain all symbols immediately.
				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(batch).NotTo(BeEmpty(), "first batch must not be empty")

				seen := make(map[string]bool)
				for _, msg := range batch {
					if name, ok := msg.MetaGet("symbol_name"); ok {
						seen[name] = true
					}
				}
				for _, sym := range symbols {
					Expect(seen).To(HaveKey(sanitizeSymbolName(sym.Name)),
						"initial sample for %q missing from first batch after Connect", sym.Name)
				}
			})

			It("Connect succeeds and logs error for unknown symbol, valid symbols still receive data", func() {
				symbols := []ads.PlcSymbol{
					{Name: syms.MasterCycleCounter, CycleTime: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
					{Name: "GVL_ProcessData.DOES_NOT_EXIST_SYMBOL", CycleTime: 100 * time.Millisecond, MaxDelay: 100 * time.Millisecond},
				}
				input := createTestInputWithCustomSymbols(cfg, "notification", symbols)
				defer input.Close(ctx)

				// Connect must succeed even though one symbol is invalid.
				// Plugin logs an error for the bad symbol but continues.
				Expect(input.Connect(ctx)).NotTo(HaveOccurred())

				// Valid symbol must still produce data.
				batch, _, err := input.ReadBatch(ctx)
				Expect(err).NotTo(HaveOccurred())

				seen := make(map[string]bool)
				for _, msg := range batch {
					if name, ok := msg.MetaGet("symbol_name"); ok {
						seen[name] = true
					}
				}
				Expect(seen).To(HaveKey(sanitizeSymbolName(syms.MasterCycleCounter)),
					"valid symbol must produce data even when another symbol fails to register")
				Expect(seen).NotTo(HaveKey(sanitizeSymbolName("GVL_ProcessData.DOES_NOT_EXIST_SYMBOL")),
					"unknown symbol must not appear in batches")
			})

			// readMasterCounter returns the current master counter from the shared session.
			// Retries until MasterCycleCounter appears — post-reconnect batches can be
			// partial while on-demand symbols re-resolve.
			readMasterCounter := func() uint64 {
				var counterStr string
				Eventually(func() bool {
					batch := readSharedBatch()
					var found bool
					counterStr, found = extractSymbolValue(batch, syms.MasterCycleCounter)
					return found
				}, 30*time.Second, 500*time.Millisecond).Should(BeTrue(),
					"MasterCycleCounter never appeared in batch within 30s")
				return mustParseUint64(counterStr)
			}

			It("fast symbol with low cycleTime/maxDelay gets many changing values", func() {
				// nFastDint: updateCycles=1, step=1 — changes every PLC cycle (10ms)
				// cycleTime=10ms, maxDelay=0 → PLC notifies as fast as possible
				symbols := []ads.PlcSymbol{
					{Name: syms.DiagNFastDint, MaxDelay: 0, CycleTime: 10 * time.Millisecond},
				}
				key := sanitizeSymbolName(syms.DiagNFastDint)

				values := collectNotifications(symbols, 3*time.Second)
				Expect(values).To(HaveKey(key))
				// At 10ms cycle over 3s, expect many notifications
				Expect(len(values[key])).To(BeNumerically(">=", 20),
					"expected >=20 notifications for fast symbol at 10ms cycle, got %d", len(values[key]))

				// Values should actually change — nFastDint changes every PLC cycle
				uniqueValues := make(map[string]bool)
				for _, v := range values[key] {
					uniqueValues[v] = true
				}
				Expect(len(uniqueValues)).To(BeNumerically(">=", 10),
					"expected >=10 unique values for fast-changing symbol, got %d unique out of %d total",
					len(uniqueValues), len(values[key]))
			})

			It("same symbol with high cycleTime gets fewer notifications", func() {
				// Same nFastDint but cycleTime=500ms — should get far fewer notifications
				symbols := []ads.PlcSymbol{
					{Name: syms.DiagNFastDint, MaxDelay: 100 * time.Millisecond, CycleTime: 500 * time.Millisecond},
				}
				key := sanitizeSymbolName(syms.DiagNFastDint)

				values := collectNotifications(symbols, 3*time.Second)
				Expect(values).To(HaveKey(key))
				// At 500ms cycle over 3s, expect roughly 6 notifications (±margin)
				Expect(len(values[key])).To(BeNumerically(">=", 3),
					"expected >=3 notifications at 500ms cycle")
				Expect(len(values[key])).To(BeNumerically("<=", 30),
					"expected <=30 notifications at 500ms cycle, got %d — rate unexpectedly high", len(values[key]))
			})

			It("slow symbol with matching cycleTime gets proportional notifications", func() {
				// nInt: updateCycles=100, step=3 — changes every 100 PLC cycles (1s)
				// cycleTime=100ms, maxDelay=0 → PLC checks every 100ms, but value only changes every ~1s
				symbols := []ads.PlcSymbol{
					{Name: syms.DiagNInt, MaxDelay: 0, CycleTime: 100 * time.Millisecond},
				}
				key := sanitizeSymbolName(syms.DiagNInt)

				values := collectNotifications(symbols, 5*time.Second)
				Expect(values).To(HaveKey(key))
				// Should still get notifications (serverCycle sends even if unchanged)
				Expect(len(values[key])).To(BeNumerically(">=", 5),
					"expected >=5 notifications for slow symbol at 100ms cycle, got %d", len(values[key]))

				// All values should parse as int
				for i, v := range values[key] {
					_, err := strconv.ParseInt(v, 10, 64)
					Expect(err).NotTo(HaveOccurred(),
						"notification %d value %q should parse as int64", i, v)
				}
			})

			It("fast vs slow cycleTime produces proportional notification rates", func() {
				// Subscribe both at different rates and verify ratio
				symbols := []ads.PlcSymbol{
					{Name: syms.DiagNInt, MaxDelay: 100 * time.Millisecond, CycleTime: 1000 * time.Millisecond}, // ~1 per second
					{Name: syms.DiagNFastDint, MaxDelay: 0, CycleTime: 100 * time.Millisecond},                  // ~10 per second
				}
				slowKey := sanitizeSymbolName(syms.DiagNInt)
				fastKey := sanitizeSymbolName(syms.DiagNFastDint)

				values := collectNotifications(symbols, 5*time.Second)
				Expect(values).To(HaveKey(slowKey))
				Expect(values).To(HaveKey(fastKey))

				slowCount := len(values[slowKey])
				fastCount := len(values[fastKey])

				Expect(fastCount).To(BeNumerically(">", slowCount),
					"fast symbol (100ms cycle) should have more notifications than slow (1000ms): fast=%d slow=%d",
					fastCount, slowCount)

				// Ratio should be roughly 10x (100ms vs 1000ms), allow wide margin
				if slowCount > 0 {
					ratio := float64(fastCount) / float64(slowCount)
					Expect(ratio).To(BeNumerically(">=", 2.0),
						"fast/slow ratio should be >=2x, got %.1f (fast=%d, slow=%d)",
						ratio, fastCount, slowCount)
				}
			})

			It("notification values match deterministic formulas", func() {
				// Read master counter first, then collect notifications, verify values fall in expected range
				startCounter := readMasterCounter()

				symbols := []ads.PlcSymbol{
					{Name: syms.DiagNFastDint, MaxDelay: 0, CycleTime: 100 * time.Millisecond},
					{Name: syms.DiagNInt, MaxDelay: 0, CycleTime: 100 * time.Millisecond},
				}
				fastKey := sanitizeSymbolName(syms.DiagNFastDint)
				intKey := sanitizeSymbolName(syms.DiagNInt)

				values := collectNotifications(symbols, 3*time.Second)

				// Read master counter after collection to bound the range
				endCounter := readMasterCounter()

				// Verify nFastDint values: updateCycles=1, step=1, 32-bit signed
				Expect(values).To(HaveKey(fastKey))
				for i, v := range values[fastKey] {
					parsed := mustParseInt64(v)
					// Value should be achievable by some counter in [startCounter, endCounter+tolerance]
					found := false
					// Check if value is in range of possible expected values
					// nFastDint = truncateToSigned(counter/1 * 1, 32) = truncateToSigned(counter, 32)
					// Rather than scanning entire range, verify the value is a valid int32
					// and that it increases monotonically (with wrapping)
					Expect(parsed).To(BeNumerically(">=", math.MinInt32),
						"notification %d: nFastDint %d outside int32 range", i, parsed)
					Expect(parsed).To(BeNumerically("<=", math.MaxInt32),
						"notification %d: nFastDint %d outside int32 range", i, parsed)

					// Spot-check: value should match formula for some counter in [startCounter, endCounter+tolerance].
					// Fine scan to avoid accepting values outside the collection window.
					for mc := startCounter; mc <= endCounter+500; mc++ {
						if ads.ExpectedIntValue(mc, 1, 1, 32) == parsed {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(),
						"notification %d: nFastDint value %d not achievable by any counter in range [%d, %d]",
						i, parsed, startCounter, endCounter)
				}

				// Verify nInt values: updateCycles=100, step=3, 16-bit signed
				Expect(values).To(HaveKey(intKey))
				for i, v := range values[intKey] {
					parsed := mustParseInt64(v)
					Expect(parsed).To(BeNumerically(">=", math.MinInt16),
						"notification %d: nInt %d outside int16 range", i, parsed)
					Expect(parsed).To(BeNumerically("<=", math.MaxInt16),
						"notification %d: nInt %d outside int16 range", i, parsed)

					// Verify value matches formula within counter range
					found := false
					for mc := startCounter; mc <= endCounter+500; mc++ {
						if ads.ExpectedIntValue(mc, 100, 3, 16) == parsed {
							found = true
							break
						}
					}
					Expect(found).To(BeTrue(),
						"notification %d: nInt value %d not achievable by any counter in range [%d, %d]",
						i, parsed, startCounter, endCounter)
				}
			})

			It("cycleTime setting controls actual notification interval", func() {
				// Subscribe same fast-changing symbol with different cycleTimes,
				// measure actual inter-notification intervals to prove cycleTime works.
				type cycleTimeCase struct {
					cycleTime      time.Duration
					collectTime    time.Duration
					expectedMedian time.Duration
					tolerancePct   float64 // how far median can deviate from expected
				}
				cases := []cycleTimeCase{
					{cycleTime: 50 * time.Millisecond, collectTime: 3 * time.Second, expectedMedian: 50 * time.Millisecond, tolerancePct: 0.5},
					{cycleTime: 200 * time.Millisecond, collectTime: 3 * time.Second, expectedMedian: 200 * time.Millisecond, tolerancePct: 0.5},
					{cycleTime: 1000 * time.Millisecond, collectTime: 5 * time.Second, expectedMedian: 1000 * time.Millisecond, tolerancePct: 0.5},
				}

				key := sanitizeSymbolName(syms.DiagNFastDint)
				medians := make([]time.Duration, len(cases))

				for i, tc := range cases {
					symbols := []ads.PlcSymbol{
						{Name: syms.DiagNFastDint, MaxDelay: 0, CycleTime: tc.cycleTime},
					}
					input := createTestInputWithCustomSymbols(cfg, "notification", symbols)

					err := input.Connect(ctx)
					Expect(err).NotTo(HaveOccurred())

					var timestamps []time.Time
					collectCtx, collectCancel := context.WithTimeout(ctx, tc.collectTime)

					for collectCtx.Err() == nil {
						batch, _, readErr := input.ReadBatch(collectCtx)
						if readErr != nil {
							break
						}
						for _, msg := range batch {
							if name, ok := msg.MetaGet("symbol_name"); ok && name == key {
								timestamps = append(timestamps, time.Now())
							}
							_ = msg
						}
					}
					collectCancel()
					input.Close(ctx)

					Expect(len(timestamps)).To(BeNumerically(">=", 3),
						"cycleTime=%dms: need >=3 notifications to compute intervals, got %d",
						tc.cycleTime, len(timestamps))

					// Compute inter-notification intervals
					var intervals []time.Duration
					for j := 1; j < len(timestamps); j++ {
						intervals = append(intervals, timestamps[j].Sub(timestamps[j-1]))
					}

					// Sort and take median
					sort.Slice(intervals, func(a, b int) bool { return intervals[a] < intervals[b] })
					median := intervals[len(intervals)/2]
					medians[i] = median

					// Verify median is within tolerance of expected
					low := time.Duration(float64(tc.expectedMedian) * (1 - tc.tolerancePct))
					high := time.Duration(float64(tc.expectedMedian) * (1 + tc.tolerancePct))

					GinkgoWriter.Printf("cycleTime=%dms: %d notifications, median interval=%v (expected %v, range [%v, %v])\n",
						tc.cycleTime, len(timestamps), median, tc.expectedMedian, low, high)

					Expect(median).To(BeNumerically(">=", low),
						"cycleTime=%dms: median interval %v too low (expected >=%v)",
						tc.cycleTime, median, low)
					Expect(median).To(BeNumerically("<=", high),
						"cycleTime=%dms: median interval %v too high (expected <=%v)",
						tc.cycleTime, median, high)
				}

				// Verify ordering: faster cycleTime → shorter median interval
				Expect(medians[0]).To(BeNumerically("<", medians[1]),
					"50ms cycleTime median (%v) should be less than 200ms (%v)", medians[0], medians[1])
				Expect(medians[1]).To(BeNumerically("<", medians[2]),
					"200ms cycleTime median (%v) should be less than 1000ms (%v)", medians[1], medians[2])
			})

			It("maxDelay=0 delivers notifications faster than maxDelay=500", func() {
				// Same symbol, same cycleTime, different maxDelay
				// maxDelay=0: deliver immediately on change
				// maxDelay=500: PLC may batch/delay up to 500ms
				symbolsFast := []ads.PlcSymbol{
					{Name: syms.DiagNFastDint, MaxDelay: 0, CycleTime: 100 * time.Millisecond},
				}
				symbolsSlow := []ads.PlcSymbol{
					{Name: syms.DiagNFastDint, MaxDelay: 500 * time.Millisecond, CycleTime: 100 * time.Millisecond},
				}
				key := sanitizeSymbolName(syms.DiagNFastDint)

				valuesFast := collectNotifications(symbolsFast, 3*time.Second)
				valuesSlow := collectNotifications(symbolsSlow, 3*time.Second)

				Expect(valuesFast).To(HaveKey(key))
				Expect(valuesSlow).To(HaveKey(key))

				// maxDelay=0 should deliver at least as many notifications as maxDelay=500ms
				// since the PLC delivers immediately vs batching with delay.
				GinkgoWriter.Printf("maxDelay=0: %d notifications, maxDelay=500: %d notifications\n",
					len(valuesFast[key]), len(valuesSlow[key]))
				Expect(len(valuesFast[key])).To(BeNumerically(">=", len(valuesSlow[key])),
					"maxDelay=0 should not produce fewer notifications than maxDelay=500ms")

				// Both should work and produce parseable values.
				for _, v := range valuesFast[key] {
					_, err := strconv.ParseInt(v, 10, 64)
					Expect(err).NotTo(HaveOccurred(), "maxDelay=0 value %q not parseable", v)
				}
				for _, v := range valuesSlow[key] {
					_, err := strconv.ParseInt(v, 10, 64)
					Expect(err).NotTo(HaveOccurred(), "maxDelay=500 value %q not parseable", v)
				}
			})
		})
	})
}

// TwinCAT 3: default runtime port 851, GVL-prefixed symbols
var _ = describeADSHardwareTests(
	"Beckhoff ADS Test Against TwinCAT 3 PLC (tc3prg)",
	"TEST_ADS_TC3_TARGET_IP", "TEST_ADS_TC3_TARGET_AMS", "TEST_ADS_TC3_RUNTIME_PORT",
	851, "TEST_ADS_TC3_ROUTE_USER", "TEST_ADS_TC3_ROUTE_PASS", "benthos-tc3.yaml",
)

// TwinCAT 2: default runtime port 801, flat namespace with program prefixes
var _ = describeADSHardwareTests(
	"Beckhoff ADS Test Against TwinCAT 2 PLC (tc3prg)",
	"TEST_ADS_TC2_TARGET_IP", "TEST_ADS_TC2_TARGET_AMS", "TEST_ADS_TC2_RUNTIME_PORT",
	801, "TEST_ADS_TC2_ROUTE_USER", "TEST_ADS_TC2_ROUTE_PASS", "benthos-tc2.yaml",
)
