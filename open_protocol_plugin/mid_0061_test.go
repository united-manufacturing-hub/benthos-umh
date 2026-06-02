// Copyright 2025 UMH Systems GmbH
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

package open_protocol_plugin_test

// Golden-file decode tests for MID 0061 rev-1 (LastTightening).
//
// These tests pin the ParseLastTightening decoder against hand-derived expected
// values computed directly from the Atlas Copco Open Protocol R2.16 Table 98
// parameter widths. They cover:
//   - 10-digit tightening id (spec-compliant hardware, R2.16 §Table 98)
//   - 4-digit tightening id (reference emulator) – the widthRest greedy-consume
//     final field handles both (Edge #13 in the VSDD spec)
//   - Exact-length PID-23 assertions: distinctive values that catch any
//     off-by-one mis-slicing in the final field (mutation killers)

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// MID0061Fixture holds the per-field values used to build a MID 0061 rev-1
// data field for testing. Every field uses its spec-mandated width; the single
// exception is TighteningID, which is supplied verbatim so tests can exercise
// both the 10-digit (spec) and 4-digit (emulator) representations without
// having to manipulate raw bytes.
type MID0061Fixture struct {
	CellID         string // 4 digits
	ChannelID      string // 2 digits
	ControllerName string // padded to 25
	VIN            string // padded to 25
	JobID          string // 2 digits
	PsetNumber     string // 3 digits
	BatchSize      string // 4 digits
	BatchCounter   string // 4 digits
	TighteningOK   string // 1 digit: "1" OK, "0" NOK
	TorqueStatus   string // 1 digit
	AngleStatus    string // 1 digit
	TorqueMin      string // 6 digits (x100)
	TorqueMax      string // 6 digits (x100)
	TorqueTarget   string // 6 digits (x100)
	TorqueActual   string // 6 digits (x100)
	AngleMin       string // 5 digits
	AngleMax       string // 5 digits
	AngleTarget    string // 5 digits
	AngleActual    string // 5 digits
	Timestamp      string // 19 chars YYYY-MM-DD:HH:MM:SS
	PsetChangeTime string // 19 chars
	BatchStatus    string // 1 digit
	TighteningID   string // verbatim (10 digits per spec; 4 per emulator)
}

// defaultFixture returns a fully-populated fixture with sensible values that
// can be overridden field-by-field in individual tests.
func defaultFixture() MID0061Fixture {
	return MID0061Fixture{
		CellID:         "0001",
		ChannelID:      "01",
		ControllerName: padRight("UMHTestSim", 25),
		VIN:            padRight("VIN0000001", 25),
		JobID:          "00",
		PsetNumber:     "001",
		BatchSize:      "0010",
		BatchCounter:   "0003",
		TighteningOK:   "1",
		TorqueStatus:   "1",
		AngleStatus:    "1",
		TorqueMin:      "004700", // 47.00 Nm
		TorqueMax:      "005300", // 53.00 Nm
		TorqueTarget:   "005000", // 50.00 Nm
		TorqueActual:   "005012", // 50.12 Nm
		AngleMin:       "00600",
		AngleMax:       "00840",
		AngleTarget:    "00720",
		AngleActual:    "00720",
		Timestamp:      "2026-06-01:12:00:00",
		PsetChangeTime: "2026-06-01:11:00:00",
		BatchStatus:    "0",
		TighteningID:   "0000000042", // 10-digit spec width by default
	}
}

// buildMID0061Data assembles the MID 0061 rev-1 data field from a fixture.
// Each field is emitted as its 2-digit parameter id followed by the fixture
// value verbatim (no padding is applied here – the fixture must carry values
// at the correct width already). TighteningID is placed last as PID 23.
func buildMID0061Data(f MID0061Fixture) string {
	fields := []string{
		"01" + f.CellID,
		"02" + f.ChannelID,
		"03" + f.ControllerName,
		"04" + f.VIN,
		"05" + f.JobID,
		"06" + f.PsetNumber,
		"07" + f.BatchSize,
		"08" + f.BatchCounter,
		"09" + f.TighteningOK,
		"10" + f.TorqueStatus,
		"11" + f.AngleStatus,
		"12" + f.TorqueMin,
		"13" + f.TorqueMax,
		"14" + f.TorqueTarget,
		"15" + f.TorqueActual,
		"16" + f.AngleMin,
		"17" + f.AngleMax,
		"18" + f.AngleTarget,
		"19" + f.AngleActual,
		"20" + f.Timestamp,
		"21" + f.PsetChangeTime,
		"22" + f.BatchStatus,
		"23" + f.TighteningID,
	}
	return strings.Join(fields, "")
}

// decodeFixture is a convenience: build → frame → parse → LastTightening.
func decodeFixture(f MID0061Fixture) (op.LastTightening, error) {
	data := buildMID0061Data(f)
	frame := op.BuildMessage(61, 1, []byte(data))
	frame = frame[:len(frame)-1] // strip NUL (FrameReader does this at runtime)
	tel, err := op.ParseTelegram(frame)
	if err != nil {
		return op.LastTightening{}, err
	}
	return op.ParseLastTightening(tel)
}

var _ = Describe("MID 0061 golden decode (rev-1)", func() {
	// -----------------------------------------------------------------------
	// Group 1: full decode with 10-digit tightening id (R2.16 spec width)
	// -----------------------------------------------------------------------
	Describe("10-digit tightening ID (spec-compliant hardware, R2.16 Table 98)", func() {
		var lt op.LastTightening

		BeforeEach(func() {
			var err error
			lt, err = decodeFixture(defaultFixture())
			Expect(err).NotTo(HaveOccurred())
		})

		It("decodes TighteningID == 42 from '0000000042'", func() {
			Expect(lt.TighteningID).To(Equal(42))
		})

		It("decodes TorqueActual ≈ 50.12 Nm (raw 005012 ÷ 100)", func() {
			Expect(lt.TorqueActual).To(BeNumerically("~", 50.12, 0.001))
		})

		It("decodes AngleActual == 720 degrees (raw 00720)", func() {
			Expect(lt.AngleActual).To(Equal(720))
		})

		It("decodes VIN trimmed to 'VIN0000001'", func() {
			Expect(lt.VIN).To(Equal("VIN0000001"))
		})

		It("sets TighteningOK == true when PID 09 == '1'", func() {
			Expect(lt.TighteningOK).To(BeTrue())
		})

		It("decodes TorqueMin ≈ 47.00 Nm", func() {
			Expect(lt.TorqueMin).To(BeNumerically("~", 47.00, 0.001))
		})

		It("decodes TorqueMax ≈ 53.00 Nm", func() {
			Expect(lt.TorqueMax).To(BeNumerically("~", 53.00, 0.001))
		})

		It("decodes TorqueTarget ≈ 50.00 Nm", func() {
			Expect(lt.TorqueTarget).To(BeNumerically("~", 50.00, 0.001))
		})
	})

	// -----------------------------------------------------------------------
	// Group 2: 4-digit tightening id (emulator / some real controllers)
	// -----------------------------------------------------------------------
	Describe("4-digit tightening ID (emulator / widthRest greedy consume, Edge #13)", func() {
		It("decodes TighteningID == 42 from '0042' (4-digit emulator format)", func() {
			f := defaultFixture()
			f.TighteningID = "0042" // emulator width
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(42))
		})

		It("decodes TighteningID == 1 from '0001' (4-digit, non-trivial leading zeros)", func() {
			f := defaultFixture()
			f.TighteningID = "0001"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(1))
		})

		It("decodes TighteningID == 9999 from '9999' (max 4-digit emulator value)", func() {
			f := defaultFixture()
			f.TighteningID = "9999"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(9999))
		})
	})

	// -----------------------------------------------------------------------
	// Group 3: exact-length PID-23 assertions (mutation killers)
	//
	// These tests ensure that ParseLastTightening consumes exactly the digits
	// provided for PID 23 – no more, no less. Any mutant that off-by-one slices
	// the final field (e.g. reads 9 or 11 bytes instead of 10, or 3 instead of
	// 4) will decode a different integer and fail at least one assertion here.
	// -----------------------------------------------------------------------
	Describe("PID-23 exact-length consumption (mutation killers)", func() {
		It("10-digit: '1234567890' decodes to exactly 1234567890 (no truncation/over-read)", func() {
			f := defaultFixture()
			f.TighteningID = "1234567890"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			// A mutant that reads only 9 digits gets 123456789; one that reads 11 would
			// either error (no extra bytes) or wrap to a different value.
			Expect(lt.TighteningID).To(Equal(1234567890))
		})

		It("10-digit: '0000000001' decodes to exactly 1 (leading-zero preservation)", func() {
			f := defaultFixture()
			f.TighteningID = "0000000001"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(1))
		})

		It("10-digit: '4294967295' decodes to exactly 4294967295 (uint32 max boundary)", func() {
			f := defaultFixture()
			f.TighteningID = "4294967295"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(4294967295))
		})

		It("4-digit: '1234' decodes to exactly 1234 (no truncation/over-read)", func() {
			f := defaultFixture()
			f.TighteningID = "1234"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			// A mutant that reads 3 digits gets 123; one that reads 5 would
			// either error or produce a wrong value.
			Expect(lt.TighteningID).To(Equal(1234))
		})

		It("4-digit: '5678' decodes to exactly 5678 (second distinctive value)", func() {
			f := defaultFixture()
			f.TighteningID = "5678"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(5678))
		})

		It("data field ends exactly after PID-23 value (no trailing bytes)", func() {
			// Verify that the data we build has NO trailing bytes after the
			// tightening id – i.e. the greedy consume gets exactly the id digits.
			f := defaultFixture()
			f.TighteningID = "0000000042"
			data := buildMID0061Data(f)
			// The last 2+10=12 bytes must be "23" + "0000000042".
			Expect(data[len(data)-12:]).To(Equal("230000000042"))
			// Decode still succeeds.
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(42))
		})
	})

	// -----------------------------------------------------------------------
	// Group 4: NOK tightening (PID-09 == "0")
	// -----------------------------------------------------------------------
	Describe("NOK result", func() {
		It("sets TighteningOK == false when PID 09 == '0'", func() {
			f := defaultFixture()
			f.TighteningOK = "0"
			lt, err := decodeFixture(f)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningOK).To(BeFalse())
		})
	})
})
