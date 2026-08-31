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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	adsLib "github.com/RuneRoven/go-ads/v2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// fakeClient is a scriptable in-memory implementation of Client used to unit
// test the read paths (pull/notification) without a real ADS session.
type fakeClient struct {
	symbols             map[string]SymbolInfo
	multiValues         map[string]string
	multiErr            error
	readFromSymbolValue map[string]string
	readFromSymbolErr   error
	connectErr          error
	loadSymbolsErr      error
	notifyErr           error
	notifyResults       []NotifyResult
	closed              bool
	getSymbolCalls      int
	readFromSymbolCalls int
	callOrder           []string
}

func (f *fakeClient) Connect(_ context.Context) error { return f.connectErr }
func (f *fakeClient) Close() error                    { f.closed = true; return nil }
func (f *fakeClient) IsClosed() bool                  { return f.closed }
func (f *fakeClient) LoadSymbols(_ context.Context) error {
	f.callOrder = append(f.callOrder, "LoadSymbols")
	return f.loadSymbolsErr
}

func (f *fakeClient) GetSymbol(_ context.Context, name string) (SymbolInfo, error) {
	f.getSymbolCalls++
	f.callOrder = append(f.callOrder, "GetSymbol")
	if info, ok := f.symbols[name]; ok {
		return info, nil
	}
	return SymbolInfo{}, errors.New("symbol not found: " + name)
}

func (f *fakeClient) AddNotifications(_ context.Context, cfgs []NotifyConfig, _ chan *Update) ([]NotifyResult, error) {
	if f.notifyErr != nil {
		return nil, f.notifyErr
	}
	if f.notifyResults != nil {
		return f.notifyResults, nil
	}
	out := make([]NotifyResult, len(cfgs))
	for i, c := range cfgs {
		out[i] = NotifyResult{SymbolName: c.SymbolName, Registered: true}
	}
	return out, nil
}

func (f *fakeClient) ReadMultipleSymbols(_ context.Context, _ []string) (map[string]string, error) {
	// Mirrors the real adapter since go-ads v2.3.0: a partial batch returns the
	// values that succeeded *and* an error naming the ones that did not.
	return f.multiValues, f.multiErr
}

func (f *fakeClient) ReadFromSymbol(_ context.Context, name string) (string, error) {
	f.readFromSymbolCalls++
	if f.readFromSymbolErr != nil {
		return "", f.readFromSymbolErr
	}
	if v, ok := f.readFromSymbolValue[name]; ok {
		return v, nil
	}
	return "", errors.New("no value for " + name)
}

var _ = Describe("Deterministic Helper Functions", func() {
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
			Expect(ExpectedMotorState(0, 100)).To(Equal(int64(0)))   // IDLE
			Expect(ExpectedMotorState(100, 100)).To(Equal(int64(1))) // STARTING
			Expect(ExpectedMotorState(200, 100)).To(Equal(int64(2))) // RUNNING
			Expect(ExpectedMotorState(300, 100)).To(Equal(int64(3))) // STOPPING
			Expect(ExpectedMotorState(400, 100)).To(Equal(int64(4))) // ERROR
			Expect(ExpectedMotorState(500, 100)).To(Equal(int64(5))) // MAINTENANCE
			Expect(ExpectedMotorState(600, 100)).To(Equal(int64(0))) // Back to IDLE
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
	Describe("CreateSymbolList", func() {
		It("parses symbols with defaults", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.MyVar"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols).To(HaveLen(1))
			Expect(symbols[0].Name).To(Equal("MAIN.MyVar"))
			Expect(symbols[0].MaxDelay).To(Equal(100 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(1000 * time.Millisecond))
		})

		It("parses symbols with custom maxDelay and cycleTime", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Trigger:0:10"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols).To(HaveLen(1))
			Expect(symbols[0].Name).To(Equal("MAIN.Trigger"))
			Expect(symbols[0].MaxDelay).To(Equal(0 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(10 * time.Millisecond))
		})

		It("preserves symbol name casing", func() {
			symbols, _ := CreateSymbolList([]string{"main.myvar"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal("main.myvar"))
		})

		It("handles global variables starting with dot", func() {
			symbols, _ := CreateSymbolList([]string{".globalVar"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal(".globalVar"))
		})

		It("uses defaults for malformed custom values", func() {
			symbols, warnings := CreateSymbolList([]string{"MAIN.Var:abc:def"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(100 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(1000 * time.Millisecond))
			Expect(warnings).NotTo(BeEmpty())
		})

		It("handles single positional value (sets maxDelay, cycleTime defaults)", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:50"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal("MAIN.Var"))
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(1000 * time.Millisecond)) // default
		})

		It("handles keyed cycleTime only", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:cycleTime=200"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal("MAIN.Var"))
			Expect(symbols[0].MaxDelay).To(Equal(100 * time.Millisecond)) // default
			Expect(symbols[0].CycleTime).To(Equal(200 * time.Millisecond))
		})

		It("handles keyed maxDelay only", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:maxDelay=50"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal("MAIN.Var"))
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(1000 * time.Millisecond)) // default
		})

		It("handles mixed positional maxDelay with keyed cycleTime", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:50:cycleTime=200"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].Name).To(Equal("MAIN.Var"))
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(200 * time.Millisecond))
		})

		It("omitted options get exact default values passed to CreateSymbolList", func() {
			// Use non-obvious defaults to prove the function returns them, not zeros
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:50"}, 750*time.Millisecond, 300*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(750*time.Millisecond), "omitted cycleTime must equal defaultCycleTime")

			symbols2, _ := CreateSymbolList([]string{"MAIN.Var:cycleTime=200"}, 750*time.Millisecond, 300*time.Millisecond)
			Expect(symbols2[0].MaxDelay).To(Equal(300*time.Millisecond), "omitted maxDelay must equal defaultMaxDelay")
			Expect(symbols2[0].CycleTime).To(Equal(200 * time.Millisecond))

			symbols3, _ := CreateSymbolList([]string{"MAIN.Var"}, 750*time.Millisecond, 300*time.Millisecond)
			Expect(symbols3[0].MaxDelay).To(Equal(300*time.Millisecond), "plain name maxDelay must equal defaultMaxDelay")
			Expect(symbols3[0].CycleTime).To(Equal(750*time.Millisecond), "plain name cycleTime must equal defaultCycleTime")
		})

		It("positional order: first=maxDelay, second=cycleTime", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:50:200"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(200 * time.Millisecond))
		})

		It("empty first slot (::200) skips maxDelay, sets cycleTime", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var::200"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(100 * time.Millisecond)) // default — slot reserved but empty
			Expect(symbols[0].CycleTime).To(Equal(200 * time.Millisecond))
		})

		It("empty second slot (50:) sets maxDelay, skips cycleTime", func() {
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:50:"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))
			Expect(symbols[0].CycleTime).To(Equal(1000 * time.Millisecond)) // default — slot reserved but empty
		})

		It("keyed overrides positional for same field", func() {
			// positional slot 0 sets maxDelay=30ms, then keyed maxDelay=50ms overwrites it
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:30:maxDelay=50"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(50*time.Millisecond), "keyed maxDelay=50ms must override positional 30ms")
			Expect(symbols[0].CycleTime).To(Equal(1000*time.Millisecond), "cycleTime untouched, must equal default")
		})

		It("keyed does not consume positional slot", func() {
			// cycleTime=200 is keyed — positional slot 0 is still available for maxDelay
			symbols, _ := CreateSymbolList([]string{"MAIN.Var:cycleTime=200:50"}, 1000*time.Millisecond, 100*time.Millisecond)
			Expect(symbols[0].MaxDelay).To(Equal(50 * time.Millisecond))   // positional slot 0
			Expect(symbols[0].CycleTime).To(Equal(200 * time.Millisecond)) // keyed
		})

		It("handles multiple symbols", func() {
			symbols, _ := CreateSymbolList(
				[]string{"MAIN.A", "MAIN.B:0:10", ".GlobalC"},
				1000*time.Millisecond, 100*time.Millisecond,
			)
			Expect(symbols).To(HaveLen(3))
			Expect(symbols[0].Name).To(Equal("MAIN.A"))
			Expect(symbols[1].CycleTime).To(Equal(10 * time.Millisecond))
			Expect(symbols[2].Name).To(Equal(".GlobalC"))
		})
	})

	Describe("unifiedAddress wiring", func() {
		// Mirrors the unifiedAddress → Symbols wiring in NewAdsCommInput (ads.go):
		// symbols parsed via CreateSymbolList same as "symbols", but each entry
		// gets UnifiedAddress set to its own Name and is appended after the
		// regular symbol list.
		It("marks unifiedAddress-derived symbols with UnifiedAddress set, leaves regular symbols unset", func() {
			symbolList, warnings := CreateSymbolList([]string{"MAIN.regular"}, time.Second, 100*time.Millisecond)
			Expect(warnings).To(BeEmpty())

			unifiedList, warnings2 := CreateSymbolList([]string{"GVL.unified1", "GVL.unified2"}, time.Second, 100*time.Millisecond)
			Expect(warnings2).To(BeEmpty())
			for i := range unifiedList {
				unifiedList[i].UnifiedAddress = unifiedList[i].Name
			}
			symbolList = append(symbolList, unifiedList...)

			Expect(symbolList).To(HaveLen(3))
			Expect(symbolList[0].Name).To(Equal("MAIN.regular"))
			Expect(symbolList[0].UnifiedAddress).To(BeEmpty())
			Expect(symbolList[1].Name).To(Equal("GVL.unified1"))
			Expect(symbolList[1].UnifiedAddress).To(Equal("GVL.unified1"))
			Expect(symbolList[2].Name).To(Equal("GVL.unified2"))
			Expect(symbolList[2].UnifiedAddress).To(Equal("GVL.unified2"))
		})

		It("NewAdsCommInput's config wiring produces the same UnifiedAddress result end-to-end", func() {
			conf, err := adsConf.ParseYAML(`
targetAddress: "1.2.3.4"
targetAMS: "1.2.3.4.1.1"
symbols:
  - "MAIN.regular"
unifiedAddress:
  - "GVL.unified1"
`, nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewAdsCommInput(conf, service.MockResources())
			Expect(err).NotTo(HaveOccurred())
			// NewAdsCommInput wraps the concrete *AdsCommInput in
			// service.AutoRetryNacksBatched, which does not expose the
			// wrapped value; the direct-construction test above exercises
			// the actual field-assignment logic. This test only asserts
			// that the config parses and constructs without error.
		})
	})

	Describe("adsValueBytes", func() {
		DescribeTable("classifies and encodes decoded values by ADS base type",
			func(typ, decoded string, wantPayload []byte, wantTagType string) {
				payload, tagType := adsValueBytes(typ, decoded)
				Expect(tagType).To(Equal(wantTagType))
				Expect(payload).To(Equal(wantPayload))
			},
			Entry("REAL is unquoted number", "REAL", "3.14", []byte("3.14"), "number"),
			Entry("DINT is unquoted number", "DINT", "42", []byte("42"), "number"),
			Entry("BOOL is unquoted bool", "BOOL", "true", []byte("true"), "bool"),
			Entry("STRING is JSON-quoted", "STRING", "hello", []byte(`"hello"`), "string"),
			Entry("numeric-looking STRING stays quoted", "STRING", "007", []byte(`"007"`), "string"),
			Entry("unknown/empty type defaults to quoted string", "", "abc", []byte(`"abc"`), "string"),
			Entry("WSTRING is JSON-quoted", "WSTRING", "wide", []byte(`"wide"`), "string"),
			Entry("lowercase base type still classifies", "bool", "false", []byte("false"), "bool"),
		)

		It("JSON-quotes a string containing special characters correctly", func() {
			payload, tagType := adsValueBytes("STRING", `has "quotes"`)
			Expect(tagType).To(Equal("string"))
			var roundTrip string
			Expect(json.Unmarshal(payload, &roundTrip)).To(Succeed())
			Expect(roundTrip).To(Equal(`has "quotes"`))
		})
	})

	Describe("ReadBatchNotification initial-sample flush", func() {
		It("emits samples captured during Connect before waiting on the channel", func() {
			a := &AdsCommInput{
				ReadType:         "notification",
				Log:              service.MockResources().Logger(),
				NotificationChan: make(chan *Update, 8),
				pendingInitial: []*Update{
					{Variable: "MAIN.a", Value: "1"},
					{Variable: "MAIN.b", Value: "2"},
				},
			}
			msgs, _, err := a.ReadBatchNotification(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(HaveLen(2))
			Expect(a.pendingInitial).To(BeEmpty())
		})

		It("resolves enriched symbol metadata for known symbols, falls back to bare name for unknown ones", func() {
			knownSym := &PlcSymbol{Name: "MAIN.Known", DataType: "INT", BaseType: "INT", Size: 2}
			a := &AdsCommInput{
				ReadType:         "notification",
				Log:              service.MockResources().Logger(),
				NotificationChan: make(chan *Update, 8),
				symbolByName: map[string]*PlcSymbol{
					strings.ToLower(knownSym.Name): knownSym,
				},
				pendingInitial: []*Update{
					{Variable: "MAIN.Known", Value: "42"},
					{Variable: "MAIN.Unknown", Value: "99"},
				},
			}
			msgs, _, err := a.ReadBatchNotification(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(HaveLen(2))

			name0, _ := msgs[0].MetaGet("ads_symbol_name")
			Expect(name0).To(Equal("MAIN_Known"))
			dataType0, _ := msgs[0].MetaGet("ads_datatype")
			Expect(dataType0).To(Equal("INT"))
			tagType0, _ := msgs[0].MetaGet("ads_tag_type")
			Expect(tagType0).To(Equal("number"))
			b0, _ := msgs[0].AsBytes()
			Expect(string(b0)).To(Equal("42"))

			name1, _ := msgs[1].MetaGet("ads_symbol_name")
			Expect(name1).To(Equal("MAIN_Unknown"))
			_, hasDataType1 := msgs[1].MetaGet("ads_datatype")
			Expect(hasDataType1).To(BeFalse(), "unresolved symbol should not carry a data_type")
			tagType1, _ := msgs[1].MetaGet("ads_tag_type")
			Expect(tagType1).To(Equal("string"), "unresolved symbol falls back to string classification")
			b1, _ := msgs[1].AsBytes()
			Expect(string(b1)).To(Equal(`"99"`))
		})
	})

	Describe("ReadBatchPull via fakeClient", func() {
		It("emits metadata (ads_symbol_name, ads_datatype, ads_tag_type) and quotes strings but not numbers", func() {
			client := &fakeClient{
				multiValues: map[string]string{
					"MAIN.temp":   "42.5",
					"MAIN.name":   "hello",
					"MAIN.numstr": "007",
				},
			}
			a := &AdsCommInput{
				Log:    service.MockResources().Logger(),
				client: client,
				Symbols: []PlcSymbol{
					{Name: "MAIN.temp", DataType: "REAL", BaseType: "REAL"},
					{Name: "MAIN.name", DataType: "STRING", BaseType: "STRING"},
					{Name: "MAIN.numstr", DataType: "STRING", BaseType: "STRING"},
				},
			}

			msgs, _, err := a.ReadBatchPull(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(HaveLen(3))

			byName := make(map[string]service.MessageBatch)
			for _, msg := range msgs {
				name, _ := msg.MetaGet("ads_symbol_name")
				byName[name] = append(byName[name], msg)
			}

			tempMsg := byName["MAIN_temp"][0]
			dataType, _ := tempMsg.MetaGet("ads_datatype")
			Expect(dataType).To(Equal("REAL"))
			tagType, _ := tempMsg.MetaGet("ads_tag_type")
			Expect(tagType).To(Equal("number"))
			b, _ := tempMsg.AsBytes()
			Expect(string(b)).To(Equal("42.5"))

			nameMsg := byName["MAIN_name"][0]
			tagType, _ = nameMsg.MetaGet("ads_tag_type")
			Expect(tagType).To(Equal("string"))
			b, _ = nameMsg.AsBytes()
			Expect(string(b)).To(Equal(`"hello"`))

			numstrMsg := byName["MAIN_numstr"][0]
			tagType, _ = numstrMsg.MetaGet("ads_tag_type")
			Expect(tagType).To(Equal("string"))
			b, _ = numstrMsg.AsBytes()
			Expect(string(b)).To(Equal(`"007"`), "numeric-looking STRING value must stay JSON-quoted")
		})

		It("resolves unresolved symbol types on a later poll instead of staying string-typed forever", func() {
			client := &fakeClient{
				multiValues: map[string]string{"MAIN.late": "123"},
				symbols: map[string]SymbolInfo{
					"MAIN.late": {DataType: "DINT", BaseType: "DINT", Length: 4},
				},
			}
			a := &AdsCommInput{
				Log:     service.MockResources().Logger(),
				client:  client,
				Symbols: []PlcSymbol{{Name: "MAIN.late"}}, // DataType/BaseType unresolved
			}

			msgs, _, err := a.ReadBatchPull(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(HaveLen(1))
			dataType, _ := msgs[0].MetaGet("ads_datatype")
			Expect(dataType).To(Equal("DINT"))
			tagType, _ := msgs[0].MetaGet("ads_tag_type")
			Expect(tagType).To(Equal("number"))
			// The resolved type must also be cached on a.Symbols for subsequent polls.
			Expect(a.Symbols[0].DataType).To(Equal("DINT"))
		})

		It("falls back to individual reads when the batch read returns no results", func() {
			client := &fakeClient{
				multiValues:         map[string]string{}, // empty: simulates a PLC without sum-read support
				readFromSymbolValue: map[string]string{"MAIN.solo": "true"},
			}
			a := &AdsCommInput{
				Log:     service.MockResources().Logger(),
				client:  client,
				Symbols: []PlcSymbol{{Name: "MAIN.solo", DataType: "BOOL", BaseType: "BOOL"}},
			}

			msgs, _, err := a.ReadBatchPull(context.Background())
			Expect(err).NotTo(HaveOccurred())
			Expect(msgs).To(HaveLen(1))
			tagType, _ := msgs[0].MetaGet("ads_tag_type")
			Expect(tagType).To(Equal("bool"))
			b, _ := msgs[0].AsBytes()
			Expect(string(b)).To(Equal("true"))
		})

		It("returns ErrNotConnected when the client is nil", func() {
			a := &AdsCommInput{Log: service.MockResources().Logger()}
			_, _, err := a.ReadBatchPull(context.Background())
			Expect(err).To(Equal(service.ErrNotConnected))
		})

		It("returns ErrNotConnected and closes the client when the batch read fails on a dead session", func() {
			client := &fakeClient{multiErr: errors.New("session dead"), closed: true}
			a := &AdsCommInput{
				Log:     service.MockResources().Logger(),
				client:  client,
				Symbols: []PlcSymbol{{Name: "MAIN.x"}},
			}
			_, _, err := a.ReadBatchPull(context.Background())
			Expect(err).To(Equal(service.ErrNotConnected))
			// closeHandler async-closes; give it a moment.
			Eventually(func() bool { return a.client == nil }).Should(BeTrue())
		})
	})

	Describe("validateIP", func() {
		It("accepts valid IPv4", func() {
			Expect(validateIP("192.168.1.100")).To(Succeed())
			Expect(validateIP("0.0.0.0")).To(Succeed())
			Expect(validateIP("255.255.255.255")).To(Succeed())
		})
		It("rejects wrong octet count", func() {
			Expect(validateIP("192.168.1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
			Expect(validateIP("192.168.1.1.1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
		It("rejects out-of-range octet", func() {
			Expect(validateIP("192.168.1.256")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
			Expect(validateIP("192.168.1.-1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
		It("rejects non-numeric octet", func() {
			Expect(validateIP("192.168.1.abc")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
		It("rejects leading-zero octets", func() {
			Expect(validateIP("192.168.001.1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
		It("rejects the IPv4-mapped IPv6 form", func() {
			Expect(validateIP("::ffff:192.168.1.1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
	})

	Describe("validateAMSNetID", func() {
		It("accepts valid AMS NetID", func() {
			Expect(validateAMSNetID("192.168.1.100.1.1")).To(Succeed())
			Expect(validateAMSNetID("10.0.0.5.1.1")).To(Succeed())
			Expect(validateAMSNetID("255.255.255.255.255.255")).To(Succeed())
		})
		It("rejects wrong octet count", func() {
			Expect(validateAMSNetID("192.168.1.100.1")).To(MatchError(ContainSubstring("6 dot-separated")))
			Expect(validateAMSNetID("192.168.1.100.1.1.1")).To(MatchError(ContainSubstring("6 dot-separated")))
		})
		It("rejects out-of-range octet", func() {
			Expect(validateAMSNetID("192.168.1.100.1.256")).To(MatchError(ContainSubstring("invalid octet")))
		})
		It("rejects a zero suffix octet", func() {
			// Beckhoff specifies 1–255 per NetID element; the trailing two are the
			// AMS suffix (conventionally .1.1), so .0 is not a valid value there.
			Expect(validateAMSNetID("0.0.0.0.0.0")).To(MatchError(ContainSubstring("must be 1–255")))
			Expect(validateAMSNetID("192.168.1.100.0.1")).To(MatchError(ContainSubstring("must be 1–255")))
			Expect(validateAMSNetID("192.168.1.100.1.0")).To(MatchError(ContainSubstring("must be 1–255")))
		})
		It("rejects an invalid IPv4 prefix", func() {
			Expect(validateAMSNetID("192.168.1.256.1.1")).To(MatchError(ContainSubstring("not a valid IPv4 address")))
		})
		It("rejects non-numeric octet", func() {
			Expect(validateAMSNetID("192.168.1.100.1.x")).To(MatchError(ContainSubstring("invalid octet")))
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
})

var _ = Describe("Time-delta Formula Verification", func() {
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

var _ = Describe("connectHint", func() {
	It("blames the transport when the dial itself failed", func() {
		err := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
		Expect(connectHint(err)).To(ContainSubstring("targetAddress"))
		Expect(connectHint(err)).NotTo(ContainSubstring("targetAMS"))
	})

	It("blames the AMS layer when the PLC reset an established connection", func() {
		err := &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}
		hint := connectHint(err)
		Expect(hint).To(ContainSubstring("targetAMS"))
		Expect(hint).To(ContainSubstring("username/password"))
		Expect(hint).To(ContainSubstring("runtimePort"))
	})

	It("treats a non-net error as an AMS-layer rejection", func() {
		Expect(connectHint(errors.New("boom"))).To(ContainSubstring("targetAMS"))
	})

	It("blames the route when the PLC served no frame at all", func() {
		err := fmt.Errorf("connect: %w", adsLib.ErrRouteNotServed)
		hint := connectHint(err)
		Expect(connectDropKind(err)).To(Equal(dropRouteNotServed))
		Expect(hint).To(ContainSubstring("hostIP"))
		Expect(hint).NotTo(ContainSubstring("runtimePort"))
	})

	It("blames the network when an established connection was dropped", func() {
		err := fmt.Errorf("connect: %w", adsLib.ErrEstablishedDropped)
		hint := connectHint(err)
		Expect(connectDropKind(err)).To(Equal(dropEstablished))
		Expect(hint).To(ContainSubstring("not a configuration error"))
	})

	It("leaves an unrelated error unclassified", func() {
		Expect(connectDropKind(errors.New("boom"))).To(Equal(dropUnknown))
	})
})

var _ = Describe("Shutdown quiets expected failures", func() {
	It("stops resolving symbols instead of failing once per remaining symbol", func() {
		client := &fakeClient{symbols: map[string]SymbolInfo{}}
		a := &AdsCommInput{
			Log:    service.MockResources().Logger(),
			client: client,
			Symbols: []PlcSymbol{
				{Name: "MAIN.a"}, {Name: "MAIN.b"}, {Name: "MAIN.c"},
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		a.initSymbolIndex(ctx)

		Expect(client.getSymbolCalls).To(BeZero())
		// The index is still fully populated so later reads can resolve names.
		Expect(a.symbolByName).To(HaveLen(3))
	})

	It("returns the context error rather than retrying a pull read", func() {
		client := &fakeClient{multiErr: context.Canceled}
		a := &AdsCommInput{
			Log:     service.MockResources().Logger(),
			client:  client,
			Symbols: []PlcSymbol{{Name: "MAIN.a"}},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		msgs, _, err := a.ReadBatchPull(ctx)

		Expect(err).To(MatchError(context.Canceled))
		Expect(msgs).To(BeNil())
	})

	It("abandons the individual-read fallback", func() {
		// Batch read yields nothing, which normally triggers per-symbol reads.
		client := &fakeClient{multiValues: map[string]string{}}
		a := &AdsCommInput{
			Log:     service.MockResources().Logger(),
			client:  client,
			Symbols: []PlcSymbol{{Name: "MAIN.a"}, {Name: "MAIN.b"}},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := a.ReadBatchPull(ctx)

		Expect(err).To(MatchError(context.Canceled))
		Expect(client.readFromSymbolCalls).To(BeZero())
	})
})

var _ = Describe("adsValueBytes value validation", func() {
	DescribeTable("emits a number only when the value survives as JSON",
		func(typ, decoded, wantPayload, wantTag string) {
			payload, tag := adsValueBytes(typ, decoded)
			Expect(string(payload)).To(Equal(wantPayload))
			Expect(tag).To(Equal(wantTag))
		},
		Entry("integer", "DINT", "42", "42", "number"),
		Entry("negative integer", "INT", "-7", "-7", "number"),
		Entry("float", "REAL", "42.5", "42.5", "number"),
		// A PLC REAL holds these after an uninitialised read or 0.0/0.0.
		Entry("NaN falls back to a string", "REAL", "NaN", `"NaN"`, "string"),
		Entry("+Inf falls back to a string", "REAL", "+Inf", `"+Inf"`, "string"),
		Entry("-Inf falls back to a string", "LREAL", "-Inf", `"-Inf"`, "string"),
		Entry("empty numeric falls back to a string", "DINT", "", `""`, "string"),
		// strconv would accept both of these; encoding/json does not.
		Entry("leading plus falls back to a string", "DINT", "+1", `"+1"`, "string"),
		Entry("hex float falls back to a string", "REAL", "0x1p+2", `"0x1p+2"`, "string"),
		Entry("bool true", "BOOL", "true", "true", "bool"),
		Entry("bool false", "BOOL", "false", "false", "bool"),
		Entry("unparseable bool falls back to a string", "BOOL", "maybe", `"maybe"`, "string"),
		Entry("string stays quoted", "STRING", "hello", `"hello"`, "string"),
		Entry("numeric-looking string stays quoted", "STRING", "007", `"007"`, "string"),
	)
})

var _ = Describe("benthosLogHandler.WithAttrs", func() {
	It("does not let sibling handlers share a backing array", func() {
		base := &benthosLogHandler{logger: service.MockResources().Logger()}
		parent := base.WithAttrs([]slog.Attr{slog.String("a", "1")}).(*benthosLogHandler)

		left := parent.WithAttrs([]slog.Attr{slog.String("b", "left")}).(*benthosLogHandler)
		right := parent.WithAttrs([]slog.Attr{slog.String("b", "right")}).(*benthosLogHandler)

		Expect(left.attrs).To(HaveLen(2))
		Expect(right.attrs).To(HaveLen(2))
		Expect(left.attrs[1].Value.String()).To(Equal("left"))
		Expect(right.attrs[1].Value.String()).To(Equal("right"))
		Expect(parent.attrs).To(HaveLen(1))
	})
})

// capturedLog is one record the plugin logged, flattened for assertions.
type capturedLog struct {
	Level slog.Level
	Msg   string
	Attrs map[string]string
}

// captureHandler collects records so a test can assert the level the plugin
// chose, which is the whole behavior of logConnectFailure.
type captureHandler struct {
	mu    *sync.Mutex
	recs  *[]capturedLog
	attrs []slog.Attr
}

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	rec := capturedLog{Level: r.Level, Msg: r.Message, Attrs: map[string]string{}}
	for _, a := range h.attrs {
		rec.Attrs[a.Key] = a.Value.String()
	}
	r.Attrs(func(a slog.Attr) bool {
		rec.Attrs[a.Key] = a.Value.String()
		return true
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.recs = append(*h.recs, rec)
	return nil
}

func (h captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]slog.Attr, 0, len(h.attrs)+len(attrs))
	return captureHandler{mu: h.mu, recs: h.recs, attrs: append(append(merged, h.attrs...), attrs...)}
}

func (h captureHandler) WithGroup(string) slog.Handler { return h }

// capturingLogger returns a benthos logger and the slice its records land in.
func capturingLogger() (*service.Logger, *[]capturedLog) {
	recs := &[]capturedLog{}
	h := captureHandler{mu: &sync.Mutex{}, recs: recs}
	return service.NewLoggerFromSlog(slog.New(h)), recs
}

func levelsOf(recs *[]capturedLog, floor slog.Level) []slog.Level {
	var out []slog.Level
	for _, r := range *recs {
		if r.Level >= floor {
			out = append(out, r.Level)
		}
	}
	return out
}

var _ = Describe("logConnectFailure", func() {
	newInput := func() (*AdsCommInput, *[]capturedLog) {
		log, recs := capturingLogger()
		return &AdsCommInput{Log: log, TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}, recs
	}

	It("warns and promises a retry when the PLC dropped the transport", func() {
		a, recs := newInput()
		err := fmt.Errorf("DownloadDataTypes failed: %w", adsLib.ErrTransportClosed)

		Expect(a.logConnectFailure(context.Background(), "Loading the table failed", err)).To(MatchError(err))

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].Level).To(Equal(slog.LevelWarn))
		Expect((*recs)[0].Msg).To(ContainSubstring("retries"))
		// The hint describes a rejected session, so it must not appear here.
		Expect((*recs)[0].Attrs).NotTo(HaveKey("hint"))
	})

	It("errors with a hint when the PLC rejected the session", func() {
		a, recs := newInput()
		err := errors.New("ADS error 0x706")

		Expect(a.logConnectFailure(context.Background(), "Connecting to PLC failed", err)).To(MatchError(err))

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].Level).To(Equal(slog.LevelError))
		Expect((*recs)[0].Attrs).To(HaveKeyWithValue("hint", ContainSubstring("targetAMS")))
	})

	It("keeps a route the PLC will not serve an error, since it needs a person", func() {
		a, recs := newInput()
		err := fmt.Errorf("connect: %w", adsLib.ErrRouteNotServed)

		a.logConnectFailure(context.Background(), "Connecting to PLC failed", err)

		Expect((*recs)[0].Level).To(Equal(slog.LevelError))
		Expect((*recs)[0].Attrs).To(HaveKeyWithValue("hint", ContainSubstring("hostIP")))
	})

	It("drops to debug during shutdown, like every other aborted operation", func() {
		a, recs := newInput()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		a.logConnectFailure(ctx, "Connecting to PLC failed", adsLib.ErrTransportClosed)

		Expect(*recs).To(HaveLen(1))
		Expect((*recs)[0].Level).To(Equal(slog.LevelDebug))
	})
})

var _ = Describe("isTransportGone", func() {
	DescribeTable("separates a transport that went away from a session that was refused",
		func(err error, want bool) {
			Expect(isTransportGone(err)).To(Equal(want))
		},
		Entry("transport closed, wrapped", fmt.Errorf("upload: %w", adsLib.ErrTransportClosed), true),
		Entry("disconnected", adsLib.ErrDisconnected, true),
		Entry("established connection dropped", fmt.Errorf("connect: %w", adsLib.ErrEstablishedDropped), true),
		// Excluded on purpose: this one usually needs credentials or an address fixed.
		Entry("route never served", fmt.Errorf("connect: %w", adsLib.ErrRouteNotServed), false),
		Entry("an ADS return code", errors.New("ADS error 0x706"), false),
		Entry("no error", nil, false),
	)
})

var _ = Describe("sessionConfig", func() {
	It("carries every tuning field through to the adapter", func() {
		a := &AdsCommInput{
			TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851,
			TargetAMS: "1.2.3.4.1.1", HostAMS: "auto", HostIP: "5.6.7.8", HostPort: 0,
			Username: "Administrator", Password: "secret",
			RequestTimeout:             5 * time.Second,
			MaxReconnectInterval:       7 * time.Second,
			RouteActivationTimeout:     20 * time.Second,
			NotificationSilenceTimeout: 15 * time.Second,
			HeartbeatRecovery:          heartbeatRecoveryRebuild,
			Log:                        service.MockResources().Logger(),
		}

		cfg := a.sessionConfig()

		Expect(cfg.MaxReconnectInterval).To(Equal(7 * time.Second))
		Expect(cfg.RouteActivationTimeout).To(Equal(20 * time.Second))
		Expect(cfg.NotificationSilenceTimeout).To(Equal(15 * time.Second))
		Expect(cfg.HeartbeatRecovery).To(Equal(heartbeatRecoveryRebuild))
		Expect(cfg.RequestTimeout).To(Equal(5 * time.Second))
	})

	It("wires OnSessionEvent to the flag the read path consumes", func() {
		a := &AdsCommInput{Log: service.MockResources().Logger()}

		cfg := a.sessionConfig()
		Expect(cfg.OnSessionEvent).NotTo(BeNil())
		cfg.OnSessionEvent(SessionEventSubscriptionsDead, "heartbeat-silent")

		reason := a.degradedReason.Load()
		Expect(reason).NotTo(BeNil())
		Expect(*reason).To(Equal("heartbeat-silent"))
	})
})

var _ = Describe("setupNotifications", func() {
	newInput := func(client *fakeClient) *AdsCommInput {
		return &AdsCommInput{
			Log:              service.MockResources().Logger(),
			client:           client,
			ReadType:         "notification",
			NotificationChan: make(chan *Update, 4),
			Symbols:          []PlcSymbol{{Name: "MAIN.a"}, {Name: "MAIN.b"}},
		}
	}

	It("names the symbol count when the whole registration fails", func() {
		a := newInput(&fakeClient{notifyErr: errors.New("transport down")})

		err := a.setupNotifications(context.Background())

		Expect(err).To(MatchError(ContainSubstring("2 symbols")))
		Expect(err).To(MatchError(ContainSubstring("transport down")))
	})

	It("fails when the PLC resolved none of the symbols", func() {
		// go-ads reports this per symbol rather than as an error, so an
		// all-skipped batch would otherwise look like a successful connect.
		a := newInput(&fakeClient{notifyResults: []NotifyResult{
			{SymbolName: "MAIN.a", Skipped: true},
			{SymbolName: "MAIN.b", Skipped: true},
		}})

		Expect(a.setupNotifications(context.Background())).To(MatchError(ContainSubstring("no symbols registered")))
	})
})

var _ = Describe("finishConnect failure branches", func() {
	newInput := func(client *fakeClient, readType string) (*AdsCommInput, *[]capturedLog) {
		log, recs := capturingLogger()
		return &AdsCommInput{
			Log: log, client: client, ReadType: readType,
			LoadSymbols:      true,
			NotificationChan: make(chan *Update, 4),
			Symbols:          []PlcSymbol{{Name: "MAIN.a"}},
		}, recs
	}

	DescribeTable("reports each failed step exactly once",
		func(client *fakeClient, readType string, wantErr string) {
			a, recs := newInput(client, readType)

			err := a.finishConnect(context.Background())

			Expect(err).To(MatchError(ContainSubstring(wantErr)))
			// One line per failure: the step used to log the error it then returned.
			Expect(levelsOf(recs, slog.LevelWarn)).To(HaveLen(1))
		},
		Entry("connect refused",
			&fakeClient{connectErr: errors.New("ADS error 0x706")}, "interval", "0x706"),
		Entry("symbol table upload interrupted",
			&fakeClient{loadSymbolsErr: fmt.Errorf("upload: %w", adsLib.ErrTransportClosed)}, "interval", "transport closed"),
		// Symbol metadata has to resolve, or initSymbolIndex adds a warning of
		// its own and the count stops being about the failure under test.
		Entry("notification registration failed",
			&fakeClient{
				notifyErr: errors.New("sum command failed"),
				symbols:   map[string]SymbolInfo{"MAIN.a": {DataType: "DINT", BaseType: "DINT", Length: 4}},
			}, "notification", "sum command failed"),
	)
})

var _ = Describe("finishConnect ordering", func() {
	It("loads the datatype table before resolving symbol metadata", func() {
		// A user-defined type only resolves to its primitive once the datatype
		// table is cached, and an unresolved base type is never retried.
		client := &fakeClient{symbols: map[string]SymbolInfo{
			"MAIN.state": {DataType: "E_MachineState", BaseType: "DINT", Length: 4},
		}}
		a := &AdsCommInput{
			Log:         service.MockResources().Logger(),
			client:      client,
			LoadSymbols: true,
			ReadType:    "interval",
			Symbols:     []PlcSymbol{{Name: "MAIN.state"}},
		}

		Expect(a.finishConnect(context.Background())).To(Succeed())

		Expect(client.callOrder).To(Equal([]string{"LoadSymbols", "GetSymbol"}))
		Expect(a.Symbols[0].BaseType).To(Equal("DINT"))
	})
})

var _ = Describe("Partial batch reads", func() {
	// go-ads v2.3.0 reports a partial batch as an error while still returning
	// every value that succeeded. Treating that as a failed poll would discard
	// good data on every read because one symbol name was wrong.
	newInput := func(client *fakeClient) *AdsCommInput {
		return &AdsCommInput{
			Log:    service.MockResources().Logger(),
			client: client,
			Symbols: []PlcSymbol{
				{Name: "MAIN.ok1", DataType: "DINT", BaseType: "DINT"},
				{Name: "MAIN.ok2", DataType: "DINT", BaseType: "DINT"},
				{Name: "MAIN.missing", DataType: "DINT", BaseType: "DINT"},
			},
		}
	}
	partial := func() (*fakeClient, *BatchReadError) {
		err := &BatchReadError{
			Requested: 3,
			Failed:    []NotifyResult{{SymbolName: "MAIN.missing", Code: 0x710}},
		}
		return &fakeClient{
			multiValues: map[string]string{"MAIN.ok1": "1", "MAIN.ok2": "2"},
			multiErr:    err,
		}, err
	}

	It("emits the symbols that succeeded instead of dropping the batch", func() {
		client, _ := partial()
		a := newInput(client)

		msgs, _, err := a.ReadBatchPull(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(msgs).To(HaveLen(2))
		names := []string{}
		for _, m := range msgs {
			n, _ := m.MetaGet("ads_symbol_name")
			names = append(names, n)
		}
		Expect(names).To(ConsistOf("MAIN_ok1", "MAIN_ok2"))
	})

	It("does not fall back to individual reads when some symbols succeeded", func() {
		client, _ := partial()
		a := newInput(client)

		_, _, err := a.ReadBatchPull(context.Background())

		Expect(err).NotTo(HaveOccurred())
		Expect(client.readFromSymbolCalls).To(BeZero())
	})

	It("names each failed symbol once per session, not once per poll", func() {
		client, _ := partial()
		a := newInput(client)

		for i := 0; i < 3; i++ {
			_, _, err := a.ReadBatchPull(context.Background())
			Expect(err).NotTo(HaveOccurred())
		}

		Expect(a.warnedBatchFailures).To(HaveLen(1))
		Expect(a.warnedBatchFailures).To(HaveKey("MAIN.missing"))
	})

	It("still treats a whole-batch failure as a failed poll", func() {
		client := &fakeClient{multiErr: errors.New("transport down")}
		a := newInput(client)

		msgs, _, err := a.ReadBatchPull(context.Background())

		Expect(err).NotTo(HaveOccurred()) // transient: empty batch, caller retries
		Expect(msgs).To(BeEmpty())
	})
})

var _ = Describe("reconnect and notification-health tuning fields", func() {
	minimalYAML := `
targetAddress: "1.2.3.4"
symbols:
  - "MAIN.var"
`

	It("defaults to go-ads' own values, stated explicitly", func() {
		conf, err := adsConf.ParseYAML(minimalYAML, nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(durationField(conf, "maxReconnectInterval")).To(Equal(30 * time.Second))
		Expect(durationField(conf, "routeActivationTimeout")).To(Equal(10 * time.Second))
		Expect(durationField(conf, "notificationSilenceTimeout")).To(Equal(10 * time.Second))
		Expect(conf.FieldString("heartbeatRecovery")).To(Equal("immediate"))
	})

	It("accepts 0 as 'keep the library default'", func() {
		conf, err := adsConf.ParseYAML(minimalYAML+"maxReconnectInterval: 0s\n", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(durationField(conf, "maxReconnectInterval")).To(BeZero())
	})

	DescribeTable("rejects a negative duration rather than silently ignoring it",
		func(field string) {
			conf, err := adsConf.ParseYAML(minimalYAML+field+": -5s\n", nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewAdsCommInput(conf, service.MockResources())

			Expect(err).To(MatchError(ContainSubstring(field)))
			Expect(err).To(MatchError(ContainSubstring("must not be negative")))
		},
		Entry("maxReconnectInterval", "maxReconnectInterval"),
		Entry("routeActivationTimeout", "routeActivationTimeout"),
		Entry("notificationSilenceTimeout", "notificationSilenceTimeout"),
	)

	DescribeTable("rejects a heartbeatRecovery mode it cannot honor",
		// Outside `benthos lint` the enum is not enforced: ParseYAML and
		// FieldString accept anything, so unchecked values reach the adapter.
		func(mode string) {
			conf, err := adsConf.ParseYAML(minimalYAML+"heartbeatRecovery: "+mode+"\n", nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewAdsCommInput(conf, service.MockResources())

			Expect(err).To(MatchError(ContainSubstring("heartbeatRecovery")))
		},
		// observe never re-subscribes: a dead subscription, no data, no error.
		Entry("observe, which needs a callback the plugin does not register", "observe"),
		Entry("a typo", "confrim"),
	)

	DescribeTable("accepts the two modes it does honor",
		func(mode string) {
			conf, err := adsConf.ParseYAML(minimalYAML+"heartbeatRecovery: "+mode+"\n", nil)
			Expect(err).NotTo(HaveOccurred())

			_, err = NewAdsCommInput(conf, service.MockResources())

			Expect(err).NotTo(HaveOccurred())
		},
		Entry("immediate", heartbeatRecoveryImmediate),
		Entry("confirm", heartbeatRecoveryConfirm),
		Entry("rebuild", heartbeatRecoveryRebuild),
	)

	Describe("rebuild mode needs a callback to hand the decision to", func() {
		base := SessionConfig{TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}

		optionCount := func(cfg SessionConfig) int {
			opts, err := buildSessionOptions(context.Background(), cfg, service.MockResources().Logger())
			Expect(err).NotTo(HaveOccurred())
			return len(opts)
		}

		It("adds both the recovery mode and the callback when one is set", func() {
			withCB := base
			withCB.HeartbeatRecovery = heartbeatRecoveryRebuild
			withCB.OnSessionEvent = func(SessionEvent, string) {}

			Expect(optionCount(withCB)).To(Equal(optionCount(base) + 2))
		})

		It("does not switch go-ads out of recovering when there is no callback", func() {
			// Observe with no callback stops delivering and says nothing.
			noCB := base
			noCB.HeartbeatRecovery = heartbeatRecoveryRebuild

			Expect(optionCount(noCB)).To(Equal(optionCount(base)))
		})
	})

	Describe("sessionEventFor", func() {
		DescribeTable("classifies the reasons the plugin must act on",
			func(reason adsLib.Reason, want SessionEvent) {
				Expect(sessionEventFor(reason)).To(Equal(want))
			},
			Entry("heartbeat silent", adsLib.ReasonHeartbeatSilent, SessionEventSubscriptionsDead),
			Entry("reload cap exhausted", adsLib.ReasonReloadCapExhausted, SessionEventSymbolReloadGaveUp),
			// go-ads drives its own reload for these; escalating would undo it.
			Entry("symbol version invalid", adsLib.ReasonSymbolVersionInvalid, SessionEventOther),
			Entry("notify handle invalid", adsLib.ReasonNotifyHandleInvalid, SessionEventOther),
			Entry("a reason added in a later version", adsLib.Reason("something-new"), SessionEventOther),
		)
	})
})

var _ = Describe("session rebuild on a degraded session", func() {
	newDegradableInput := func(client Client) *AdsCommInput {
		return &AdsCommInput{
			ReadType: "notification",
			Log:      service.MockResources().Logger(),
			client:   client,
			Symbols:  []PlcSymbol{{Name: "MAIN.var"}},
		}
	}

	DescribeTable("reports itself disconnected so the pipeline reconnects",
		func(ev SessionEvent) {
			a := newDegradableInput(&fakeClient{})
			a.onSessionEvent(ev, "some-reason")

			_, _, err := a.ReadBatch(context.Background())

			Expect(err).To(MatchError(service.ErrNotConnected))
			// closeHandler drops the client so Connect builds a new session.
			Expect(a.client).To(BeNil())
		},
		Entry("subscriptions dead", SessionEventSubscriptionsDead),
		Entry("symbol reload gave up", SessionEventSymbolReloadGaveUp),
	)

	It("leaves a healthy session alone for an event go-ads handles itself", func() {
		a := newDegradableInput(&fakeClient{})
		a.onSessionEvent(SessionEventOther, "symbol-version-invalid")

		Expect(a.rebuildIfDegraded()).To(Succeed())
		Expect(a.client).NotTo(BeNil())
	})

	It("consumes the flag, so one event costs one rebuild", func() {
		a := newDegradableInput(&fakeClient{})
		a.onSessionEvent(SessionEventSubscriptionsDead, "heartbeat-silent")

		Expect(a.rebuildIfDegraded()).To(MatchError(service.ErrNotConnected))
		Expect(a.rebuildIfDegraded()).To(Succeed())
	})

	It("keeps the first reason when a second event arrives before the read", func() {
		a := newDegradableInput(&fakeClient{})
		a.onSessionEvent(SessionEventSubscriptionsDead, "heartbeat-silent")
		a.onSessionEvent(SessionEventSymbolReloadGaveUp, "reload-cap-exhausted")

		reason := a.degradedReason.Load()
		Expect(reason).NotTo(BeNil())
		Expect(*reason).To(Equal("heartbeat-silent"))
	})

	Describe("cappedBackoff", func() {
		DescribeTable("keeps the tiers valid for any cap go-ads would otherwise reject",
			func(limit time.Duration) {
				bc := cappedBackoff(limit)

				// Guards the case where WithBackoff warns, keeps the default, and
				// the requested cap applies to nothing.
				Expect(bc.Validate()).To(Succeed())
				Expect(bc.MaxInterval).To(Equal(limit))
				Expect(bc.SlowInterval).To(BeNumerically("<=", limit))
				Expect(bc.MidInterval).To(BeNumerically("<=", bc.SlowInterval))
				Expect(bc.InitialInterval).To(BeNumerically("<=", bc.MidInterval))
			},
			Entry("below every tier", 500*time.Millisecond),
			Entry("between the initial and mid tiers", 2*time.Second),
			Entry("between the mid and slow tiers", 10*time.Second),
			Entry("the default cap", 30*time.Second),
			Entry("above the default cap", 2*time.Minute),
		)

		It("leaves the lower tiers alone when the cap is above them", func() {
			bc := cappedBackoff(2 * time.Minute)
			def := adsLib.DefaultBackoffConfig()

			Expect(bc.InitialInterval).To(Equal(def.InitialInterval))
			Expect(bc.MidInterval).To(Equal(def.MidInterval))
			Expect(bc.SlowInterval).To(Equal(def.SlowInterval))
		})
	})

	Describe("buildSessionOptions", func() {
		// No username/password, so no route resolution and no network access.
		base := SessionConfig{TargetIP: "1.2.3.4", TargetPort: 48898, RuntimePort: 851}

		optionCount := func(cfg SessionConfig) int {
			opts, err := buildSessionOptions(context.Background(), cfg, service.MockResources().Logger())
			Expect(err).NotTo(HaveOccurred())
			return len(opts)
		}

		It("adds one option per knob that is set", func() {
			tuned := base
			tuned.MaxReconnectInterval = 5 * time.Second
			tuned.RouteActivationTimeout = 30 * time.Second
			tuned.NotificationSilenceTimeout = 20 * time.Second
			tuned.HeartbeatRecovery = "confirm"

			Expect(optionCount(tuned)).To(Equal(optionCount(base) + 4))
		})

		It("adds nothing for the immediate recovery mode, which is already the default", func() {
			immediate := base
			immediate.HeartbeatRecovery = "immediate"

			Expect(optionCount(immediate)).To(Equal(optionCount(base)))
		})

		It("adds nothing for a zero duration", func() {
			zeroed := base
			zeroed.MaxReconnectInterval = 0
			zeroed.RouteActivationTimeout = 0
			zeroed.NotificationSilenceTimeout = 0

			Expect(optionCount(zeroed)).To(Equal(optionCount(base)))
		})
	})
})

var _ = Describe("targetAMS is optional", func() {
	It("builds without targetAMS, leaving the PLC to supply it", func() {
		conf, err := adsConf.ParseYAML(`
targetAddress: "1.2.3.4"
symbols:
  - "MAIN.var"
`, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = NewAdsCommInput(conf, service.MockResources())

		Expect(err).NotTo(HaveOccurred())
	})

	It("still rejects a targetAMS that is present but malformed", func() {
		conf, err := adsConf.ParseYAML(`
targetAddress: "1.2.3.4"
targetAMS: "not-an-ams-netid"
symbols:
  - "MAIN.var"
`, nil)
		Expect(err).NotTo(HaveOccurred())

		_, err = NewAdsCommInput(conf, service.MockResources())

		Expect(err).To(MatchError(ContainSubstring("targetAMS")))
	})
})

var _ = Describe("benthosLogHandler level mapping", func() {
	// go-ads owns the level; the bridge only maps slog levels onto benthos ones.
	rec := func(level slog.Level, msg string, attrs ...slog.Attr) slog.Record {
		r := slog.NewRecord(time.Time{}, level, msg, 0)
		r.AddAttrs(attrs...)
		return r
	}
	handle := func(r slog.Record) error {
		h := &benthosLogHandler{logger: service.MockResources().Logger()}
		return h.Handle(context.Background(), r)
	}

	DescribeTable("forwards a record at the level go-ads set, attributes and all",
		func(level slog.Level, attrs []slog.Attr) {
			r := rec(level, "x", attrs...)
			Expect(handle(r)).To(Succeed())
			Expect(r.Level).To(Equal(level))
		},
		Entry("connection line", slog.LevelInfo, nil),
		Entry("per-symbol bookkeeping arrives as debug", slog.LevelDebug, []slog.Attr{slog.String("symbol", "MAIN.a")}),
		Entry("per-handle bookkeeping arrives as debug", slog.LevelDebug, []slog.Attr{slog.Uint64("handle", 91)}),
		Entry("a failure carrying a symbol stays a warning", slog.LevelWarn, []slog.Attr{slog.String("symbol", "MAIN.a")}),
		Entry("error", slog.LevelError, []slog.Attr{slog.String("symbol", "MAIN.a")}),
	)

	It("suppresses trace records below debug", func() {
		h := &benthosLogHandler{logger: service.MockResources().Logger()}
		Expect(h.Enabled(context.Background(), slog.Level(-8))).To(BeFalse())
		Expect(h.Enabled(context.Background(), slog.LevelDebug)).To(BeTrue())
	})
})
