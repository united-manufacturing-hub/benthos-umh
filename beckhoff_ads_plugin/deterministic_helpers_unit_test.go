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
	"math"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Deterministic Helper Functions", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	Describe("ExpectedCounterValue", func() {
		It("computes basic counter values", func() {
			Expect(ExpectedCounterValue(1000, 100, 1)).To(Equal(uint64(10)))
			Expect(ExpectedCounterValue(1000, 100, 3)).To(Equal(uint64(30)))
			Expect(ExpectedCounterValue(999, 100, 1)).To(Equal(uint64(9)))
			Expect(ExpectedCounterValue(0, 100, 1)).To(Equal(uint64(0)))
		})

		It("handles zero updateCycles by defaulting to 1", func() {
			Expect(ExpectedCounterValue(500, 0, 3)).To(Equal(uint64(1500)))
		})
	})

	Describe("Integer truncation", func() {
		DescribeTable("ExpectedIntValue truncates to signed width",
			func(masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int, expected int64) {
				Expect(ExpectedIntValue(masterCounter, updateCycles, stepSize, bitWidth)).To(Equal(expected))
			},
			// 8-bit signed: 128 truncates to -128, 256 truncates to 0
			Entry("SINT truncates 128 to -128", uint64(12800), uint64(100), uint64(1), 8, int64(-128)),
			Entry("SINT at 127", uint64(12700), uint64(100), uint64(1), 8, int64(127)),
			Entry("SINT wraps 256 to 0", uint64(25600), uint64(100), uint64(1), 8, int64(0)),
			// 16-bit signed
			Entry("INT basic", uint64(1000), uint64(100), uint64(3), 16, int64(30)),
			// 32-bit signed
			Entry("DINT basic", uint64(1000), uint64(100), uint64(7), 32, int64(70)),
		)

		DescribeTable("ExpectedUintValue truncates to unsigned width",
			func(masterCounter uint64, updateCycles uint64, stepSize uint64, bitWidth int, expected uint64) {
				Expect(ExpectedUintValue(masterCounter, updateCycles, stepSize, bitWidth)).To(Equal(expected))
			},
			Entry("BYTE wraps at 256", uint64(25600), uint64(100), uint64(1), 8, uint64(0)),
			Entry("BYTE at 255", uint64(25500), uint64(100), uint64(1), 8, uint64(255)),
			Entry("WORD basic", uint64(1000), uint64(100), uint64(1), 16, uint64(10)),
		)
	})

	Describe("ExpectedBoolToggle", func() {
		It("starts as false and toggles", func() {
			Expect(ExpectedBoolToggle(0, 50)).To(BeFalse())
			Expect(ExpectedBoolToggle(49, 50)).To(BeFalse())
			Expect(ExpectedBoolToggle(50, 50)).To(BeTrue())
			Expect(ExpectedBoolToggle(99, 50)).To(BeTrue())
			Expect(ExpectedBoolToggle(100, 50)).To(BeFalse())
		})
	})

	Describe("ExpectedSawtoothFloat", func() {
		It("produces sawtooth pattern for fReal", func() {
			// masterCounter=10000, updateCycles=100 → counterVal=100, 100%1000=100, 100*0.1=10.0
			Expect(ExpectedSawtoothFloat(10000, 100, 1000, 0.1)).To(BeNumerically("~", 10.0, 0.01))
			// masterCounter=0 → counterVal=0, 0%1000=0, 0*0.1=0.0
			Expect(ExpectedSawtoothFloat(0, 100, 1000, 0.1)).To(BeNumerically("~", 0.0, 0.01))
		})

		It("wraps at mod boundary", func() {
			// counterVal = 1000 → 1000%1000 = 0
			Expect(ExpectedSawtoothFloat(100000, 100, 1000, 0.1)).To(BeNumerically("~", 0.0, 0.01))
		})
	})

	Describe("ExpectedMotorState", func() {
		It("cycles through 0-5", func() {
			Expect(ExpectedMotorState(0, 100)).To(Equal(int64(0)))     // IDLE
			Expect(ExpectedMotorState(100, 100)).To(Equal(int64(1)))   // STARTING
			Expect(ExpectedMotorState(200, 100)).To(Equal(int64(2)))   // RUNNING
			Expect(ExpectedMotorState(300, 100)).To(Equal(int64(3)))   // STOPPING
			Expect(ExpectedMotorState(400, 100)).To(Equal(int64(4)))   // ERROR
			Expect(ExpectedMotorState(500, 100)).To(Equal(int64(5)))   // MAINTENANCE
			Expect(ExpectedMotorState(600, 100)).To(Equal(int64(0)))   // Back to IDLE
		})
	})

	Describe("ExpectedMotorEnabled", func() {
		It("is true for STARTING, RUNNING, MAINTENANCE", func() {
			Expect(ExpectedMotorEnabled(0, 100)).To(BeFalse())   // IDLE
			Expect(ExpectedMotorEnabled(100, 100)).To(BeTrue())  // STARTING
			Expect(ExpectedMotorEnabled(200, 100)).To(BeTrue())  // RUNNING
			Expect(ExpectedMotorEnabled(300, 100)).To(BeFalse()) // STOPPING
			Expect(ExpectedMotorEnabled(400, 100)).To(BeFalse()) // ERROR
			Expect(ExpectedMotorEnabled(500, 100)).To(BeTrue())  // MAINTENANCE
		})
	})

	Describe("ExpectedMotorError", func() {
		It("is true only for ERROR state", func() {
			Expect(ExpectedMotorError(400, 100)).To(BeTrue())
			Expect(ExpectedMotorError(0, 100)).To(BeFalse())
			Expect(ExpectedMotorError(200, 100)).To(BeFalse())
		})
	})

	Describe("ExpectedLwordROL", func() {
		It("rotates bit left", func() {
			Expect(ExpectedLwordROL(0, 100)).To(Equal(uint64(1)))
			Expect(ExpectedLwordROL(100, 100)).To(Equal(uint64(2)))
			Expect(ExpectedLwordROL(200, 100)).To(Equal(uint64(4)))
			// After 64 rotations, wraps back
			Expect(ExpectedLwordROL(6400, 100)).To(Equal(uint64(1)))
		})
	})

	Describe("ExpectedStringCycler", func() {
		It("produces correct pattern", func() {
			Expect(ExpectedStringCycler(0, 100, "Machine")).To(Equal("Machine_000_A"))
			Expect(ExpectedStringCycler(100, 100, "Machine")).To(Equal("Machine_000_B"))
			Expect(ExpectedStringCycler(2500, 100, "Machine")).To(Equal("Machine_000_Z"))
			Expect(ExpectedStringCycler(2600, 100, "Machine")).To(Equal("Machine_001_A"))
		})
	})

	Describe("ExpectedArrayCounter", func() {
		It("computes array element values", func() {
			// index 0: step=1, updateCycles=100
			Expect(ExpectedArrayCounter(10000, 0)).To(Equal(int64(100)))
			// index 2: step=3, updateCycles=100
			Expect(ExpectedArrayCounter(10000, 2)).To(Equal(int64(300)))
		})
	})

	Describe("ExpectedArrayMeasurement", func() {
		It("computes sawtooth measurement", func() {
			// index 0: modVal=1000, divisor=10
			val := ExpectedArrayMeasurement(10000, 0)
			// counterVal = 100, 100%1000=100, 100/10=10.0
			Expect(val).To(BeNumerically("~", 10.0, 0.01))
		})
	})

	Describe("Production stats", func() {
		It("computes parts produced and yield", func() {
			// masterCounter=10000 → nUpdates=100
			Expect(ExpectedProductionPartsProduced(10000)).To(Equal(uint64(200)))
			Expect(ExpectedProductionPartsRejected(10000)).To(Equal(uint64(10)))
			Expect(ExpectedProductionYield(10000)).To(BeNumerically("~", 95.0, 0.01))
			Expect(ExpectedBatchNumber(10000)).To(Equal(uint64(2)))
		})

		It("returns 100% yield when no parts produced", func() {
			Expect(ExpectedProductionYield(0)).To(Equal(100.0))
		})
	})

	Describe("VerifyIntWithTolerance", func() {
		It("matches exact value", func() {
			expected := ExpectedIntValue(1000, 100, 3, 16)
			Expect(VerifyIntWithTolerance(expected, 1000, 100, 3, 16)).To(BeTrue())
		})

		It("matches within tolerance window", func() {
			// Value at counter+3 should also match
			expected := ExpectedIntValue(1003, 100, 3, 16)
			Expect(VerifyIntWithTolerance(expected, 1000, 100, 3, 16)).To(BeTrue())
		})

		It("rejects out-of-tolerance values", func() {
			Expect(VerifyIntWithTolerance(99999, 1000, 100, 3, 16)).To(BeFalse())
		})
	})

	Describe("VerifyFloatWithTolerance", func() {
		It("matches within epsilon", func() {
			calc := func(mc uint64) float64 {
				return ExpectedSawtoothFloat(mc, 100, 1000, 0.1)
			}
			actual := calc(1000) + 0.001
			Expect(VerifyFloatWithTolerance(actual, 1000, calc, 0.01)).To(BeTrue())
		})
	})

	Describe("VerifyStringWithTolerance", func() {
		It("matches within tolerance", func() {
			calc := func(mc uint64) string {
				return ExpectedStringCycler(mc, 100, "Test")
			}
			actual := calc(1002)
			Expect(VerifyStringWithTolerance(actual, 1000, calc)).To(BeTrue())
		})
	})

	Describe("Internal utilities", func() {
		It("truncateToSigned handles edge cases", func() {
			Expect(truncateToSigned(255, 8)).To(Equal(int64(-1)))
			Expect(truncateToSigned(127, 8)).To(Equal(int64(127)))
			Expect(truncateToSigned(128, 8)).To(Equal(int64(-128)))
		})

		It("truncateToUnsigned masks correctly", func() {
			Expect(truncateToUnsigned(256, 8)).To(Equal(uint64(0)))
			Expect(truncateToUnsigned(255, 8)).To(Equal(uint64(255)))
		})

		It("rotateLeft64 rotates correctly", func() {
			Expect(rotateLeft64(1, 0)).To(Equal(uint64(1)))
			Expect(rotateLeft64(1, 1)).To(Equal(uint64(2)))
			Expect(rotateLeft64(1, 63)).To(Equal(uint64(1 << 63)))
			Expect(rotateLeft64(1, 64)).To(Equal(uint64(1))) // wraps
		})
	})
})

var _ = Describe("Plugin Internal Functions", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	Describe("CreateSymbolList", func() {
		It("parses symbols with defaults", func() {
			symbols := CreateSymbolList([]string{"MAIN.MyVar"}, 1000, 100, false)
			Expect(symbols).To(HaveLen(1))
			Expect(symbols[0].Name).To(Equal("MAIN.MyVar"))
			Expect(symbols[0].MaxDelay).To(Equal(100))
			Expect(symbols[0].CycleTime).To(Equal(1000))
		})

		It("parses symbols with custom maxDelay and cycleTime", func() {
			symbols := CreateSymbolList([]string{"MAIN.Trigger:0:10"}, 1000, 100, false)
			Expect(symbols).To(HaveLen(1))
			Expect(symbols[0].Name).To(Equal("MAIN.Trigger"))
			Expect(symbols[0].MaxDelay).To(Equal(0))
			Expect(symbols[0].CycleTime).To(Equal(10))
		})

		It("applies uppercase conversion", func() {
			symbols := CreateSymbolList([]string{"main.myvar"}, 1000, 100, true)
			Expect(symbols[0].Name).To(Equal("MAIN.MYVAR"))
		})

		It("handles global variables starting with dot", func() {
			symbols := CreateSymbolList([]string{".globalVar"}, 1000, 100, false)
			Expect(symbols[0].Name).To(Equal(".globalVar"))
		})

		It("uses defaults for malformed custom values", func() {
			symbols := CreateSymbolList([]string{"MAIN.Var:abc:def"}, 1000, 100, false)
			Expect(symbols[0].MaxDelay).To(Equal(100))
			Expect(symbols[0].CycleTime).To(Equal(1000))
		})

		It("handles single colon (not two)", func() {
			symbols := CreateSymbolList([]string{"MAIN.Var:50"}, 1000, 100, false)
			Expect(symbols[0].Name).To(Equal("MAIN.Var"))
			Expect(symbols[0].MaxDelay).To(Equal(100))  // defaults used
			Expect(symbols[0].CycleTime).To(Equal(1000)) // defaults used
		})

		It("handles multiple symbols", func() {
			symbols := CreateSymbolList(
				[]string{"MAIN.A", "MAIN.B:0:10", ".GlobalC"},
				1000, 100, false,
			)
			Expect(symbols).To(HaveLen(3))
			Expect(symbols[0].Name).To(Equal("MAIN.A"))
			Expect(symbols[1].CycleTime).To(Equal(10))
			Expect(symbols[2].Name).To(Equal(".GlobalC"))
		})
	})

	Describe("sanitize", func() {
		It("replaces dots with underscores", func() {
			Expect(sanitize("MAIN.MyVar")).To(Equal("MAIN_MyVar"))
		})

		It("preserves alphanumeric and dashes", func() {
			Expect(sanitize("my-var_123")).To(Equal("my-var_123"))
		})

		It("replaces special characters", func() {
			Expect(sanitize("MAIN.MyVar[0]")).To(Equal("MAIN_MyVar_0_"))
		})
	})

	Describe("isLikelyContainerIP", func() {
		It("detects Docker bridge IPs", func() {
			Expect(isLikelyContainerIP("172.17.0.2")).To(BeTrue())
			Expect(isLikelyContainerIP("172.18.0.1")).To(BeTrue())
			Expect(isLikelyContainerIP("172.31.255.255")).To(BeTrue())
		})

		It("detects pod network IPs", func() {
			Expect(isLikelyContainerIP("10.0.0.1")).To(BeTrue())
			Expect(isLikelyContainerIP("10.244.0.5")).To(BeTrue())
		})

		It("detects CGNAT IPs", func() {
			Expect(isLikelyContainerIP("100.64.0.1")).To(BeTrue())
			Expect(isLikelyContainerIP("100.127.255.255")).To(BeTrue())
		})

		It("rejects normal IPs", func() {
			Expect(isLikelyContainerIP("192.168.1.100")).To(BeFalse())
			Expect(isLikelyContainerIP("172.16.0.1")).To(BeFalse())
			Expect(isLikelyContainerIP("100.128.0.1")).To(BeFalse())
		})

		It("handles invalid input", func() {
			Expect(isLikelyContainerIP("not-an-ip")).To(BeFalse())
			Expect(isLikelyContainerIP("")).To(BeFalse())
		})
	})
})

var _ = Describe("Time-delta Formula Verification", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	Describe("value delta over simulated time", func() {
		It("nInt increases by expected step*cycles over 2s", func() {
			// nInt: updateCycles=100, step=3, 16-bit
			// 200 cycles = 2s at 10ms PLC task cycle
			counter0 := uint64(10000)
			counter1 := uint64(10200)
			nInt0 := ExpectedIntValue(counter0, 100, 3, 16)
			nInt1 := ExpectedIntValue(counter1, 100, 3, 16)
			expectedDelta := int64(3) * int64((counter1-counter0)/100)
			Expect(nInt1 - nInt0).To(Equal(expectedDelta))
		})

		It("nByte wraps correctly across 256 boundary", func() {
			// At counter=25500, counterVal=255, byte=255
			// At counter=25600, counterVal=256, byte=0 (wraps)
			val255 := ExpectedUintValue(25500, 100, 1, 8)
			val0 := ExpectedUintValue(25600, 100, 1, 8)
			Expect(val255).To(Equal(uint64(255)))
			Expect(val0).To(Equal(uint64(0)))
		})

		It("slow variable stays constant for sub-interval counters", func() {
			// updateCycles=100: counter 10000 and 10050 both floor-divide to 100
			val0 := ExpectedIntValue(10000, 100, 3, 16)
			val50 := ExpectedIntValue(10050, 100, 3, 16)
			Expect(val0).To(Equal(val50))
		})

		It("fast variable changes every cycle", func() {
			// updateCycles=1, step=1: every counter increment changes the value
			val0 := ExpectedIntValue(1000, 1, 1, 16)
			val1 := ExpectedIntValue(1001, 1, 1, 16)
			Expect(val0).NotTo(Equal(val1))
		})
	})
})

var _ = Describe("Simulated Poll Rate Transitions", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	It("nInt shows ~2 transitions over 200 cycles", func() {
		// nInt: updateCycles=100, step=3
		// 200 counter increments = 200 cycles
		// Value changes when floor(counter/100) changes → every 100 cycles → 2 transitions
		baseCounter := uint64(10000)
		transitions := 0
		prevVal := ExpectedIntValue(baseCounter, 100, 3, 16)
		for i := uint64(1); i <= 200; i++ {
			val := ExpectedIntValue(baseCounter+i, 100, 3, 16)
			if val != prevVal {
				transitions++
				prevVal = val
			}
		}
		Expect(transitions).To(Equal(2))
	})

	It("nFastDint changes almost every cycle", func() {
		// updateCycles=1, step=1: changes every PLC cycle
		// Simulating 50ms polls (5 cycles apart) over 20 samples
		baseCounter := uint64(10000)
		transitions := 0
		prevVal := ExpectedIntValue(baseCounter, 1, 1, 32)
		for i := 1; i < 20; i++ {
			val := ExpectedIntValue(baseCounter+uint64(i*5), 1, 1, 32)
			if val != prevVal {
				transitions++
				prevVal = val
			}
		}
		// Every sample should differ (5 cycles apart with updateCycles=1)
		Expect(transitions).To(Equal(19))
	})

	It("nByte stays stable within one update interval", func() {
		// updateCycles=100: within a single 100-cycle window, value is constant
		baseCounter := uint64(10000) // starts at exactly an update boundary
		firstVal := ExpectedUintValue(baseCounter, 100, 1, 8)
		for i := uint64(1); i < 100; i++ {
			val := ExpectedUintValue(baseCounter+i, 100, 1, 8)
			Expect(val).To(Equal(firstVal),
				"nByte should be constant within update interval, but changed at offset %d", i)
		}
	})
})

var _ = Describe("Counter Reset Period Handling", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	It("formulas produce valid values near counterResetPeriod boundary", func() {
		// Should not panic or overflow
		val := ExpectedIntValue(counterResetPeriod-1, 100, 3, 16)
		Expect(val).To(BeNumerically(">=", int64(math.MinInt16)))
		Expect(val).To(BeNumerically("<=", int64(math.MaxInt16)))

		val0 := ExpectedIntValue(0, 100, 3, 16)
		Expect(val0).To(Equal(int64(0)))

		val1 := ExpectedIntValue(1, 100, 3, 16)
		Expect(val1).To(Equal(int64(0))) // 1/100 = 0
	})

	It("tolerance window works near reset boundary", func() {
		// Near the reset period, VerifyIntWithTolerance should still work
		expected := ExpectedIntValue(counterResetPeriod-2, 100, 3, 16)
		Expect(VerifyIntWithTolerance(expected, counterResetPeriod-2, 100, 3, 16)).To(BeTrue())
	})

	It("all formula types handle counterResetPeriod without panic", func() {
		mc := uint64(counterResetPeriod - 1)
		// These should all execute without panic
		_ = ExpectedCounterValue(mc, 100, 1)
		_ = ExpectedIntValue(mc, 100, 3, 16)
		_ = ExpectedUintValue(mc, 100, 1, 8)
		_ = ExpectedBoolToggle(mc, 50)
		_ = ExpectedSawtoothFloat(mc, 100, 1000, 0.1)
		_ = ExpectedSensorValue(mc, 100, 120.0, 1000)
		_ = ExpectedSensorValid(mc, 100, 1000)
		_ = ExpectedAlarmSeverity(mc, 100, 1000)
		_ = ExpectedMotorSpeed(mc, 100)
		_ = ExpectedMotorState(mc, 100)
		_ = ExpectedMotorEnabled(mc, 100)
		_ = ExpectedMotorError(mc, 100)
		_ = ExpectedMotorTorque(mc, 100)
		_ = ExpectedLwordROL(mc, 100)
		_ = ExpectedStringCycler(mc, 100, "Machine")
		_ = ExpectedArrayCounter(mc, 0)
		_ = ExpectedArrayMeasurement(mc, 0)
		_ = ExpectedProductionPartsProduced(mc)
		_ = ExpectedProductionYield(mc)
		_ = ExpectedBatchNumber(mc)
	})
})

var _ = Describe("Multi-symbol Consistency", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_ADS_UNITTEST") == "" {
			Skip("Skipping: TEST_ADS_UNITTEST not set")
		}
	})

	It("all slow symbols produce deterministic values at the same counter", func() {
		mc := uint64(50000)

		// Each call with the same counter should always return the same value
		nByte1 := ExpectedUintValue(mc, 100, 1, 8)
		nByte2 := ExpectedUintValue(mc, 100, 1, 8)
		Expect(nByte1).To(Equal(nByte2))

		nInt1 := ExpectedIntValue(mc, 100, 3, 16)
		nInt2 := ExpectedIntValue(mc, 100, 3, 16)
		Expect(nInt1).To(Equal(nInt2))

		nDint1 := ExpectedIntValue(mc, 100, 7, 32)
		nDint2 := ExpectedIntValue(mc, 100, 7, 32)
		Expect(nDint1).To(Equal(nDint2))

		fReal1 := ExpectedSawtoothFloat(mc, 100, 1000, 0.1)
		fReal2 := ExpectedSawtoothFloat(mc, 100, 1000, 0.1)
		Expect(fReal1).To(Equal(fReal2))
	})

	It("values are consistent between related formulas", func() {
		mc := uint64(50000)

		// Motor torque should be speed * 0.1
		speed := ExpectedMotorSpeed(mc, 100)
		torque := ExpectedMotorTorque(mc, 100)
		Expect(torque).To(BeNumerically("~", speed*0.1, 1e-9))

		// Motor enabled/error should be consistent with state
		state := ExpectedMotorState(mc, 100)
		enabled := ExpectedMotorEnabled(mc, 100)
		isError := ExpectedMotorError(mc, 100)

		if state == 1 || state == 2 || state == 5 {
			Expect(enabled).To(BeTrue(), "state %d should be enabled", state)
		} else {
			Expect(enabled).To(BeFalse(), "state %d should not be enabled", state)
		}
		Expect(isError).To(Equal(state == 4))
	})

	It("production yield is consistent with parts produced/rejected", func() {
		mc := uint64(100000)
		produced := ExpectedProductionPartsProduced(mc)
		rejected := ExpectedProductionPartsRejected(mc)
		yield := ExpectedProductionYield(mc)

		expectedYield := 100.0 - (float64(rejected) * 100.0 / float64(produced))
		Expect(yield).To(BeNumerically("~", expectedYield, 1e-9))
	})
})

// Silence unused import warning for math package used in tests
var _ = math.Abs
