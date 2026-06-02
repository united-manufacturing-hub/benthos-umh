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

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

// build0061Data assembles a MID 0061 Rev 1 (last tightening result) data field
// in the parameter-ID format, mirroring the Atlas Copco specification and the
// reference emulator: each field is a 2-digit parameter ID followed by a
// fixed-width ASCII value. okStatus toggles parameter 09 (tightening status).
func build0061Data(okStatus bool) string {
	status := "0"
	if okStatus {
		status = "1"
	}
	fields := []string{
		"01" + "0001",                     // cell id
		"02" + "01",                       // channel id
		"03" + padRight("UMHTestSim", 25), // controller name
		"04" + padRight("VIN123", 25),     // VIN
		"05" + "00",                       // job id
		"06" + "001",                      // pset number
		"07" + "0005",                     // batch size
		"08" + "0002",                     // batch counter
		"09" + status,                     // tightening status (1 = OK)
		"10" + "1",                        // torque status
		"11" + "1",                        // angle status
		"12" + "004700",                   // torque min  (47.00 Nm, x100)
		"13" + "005300",                   // torque max  (53.00 Nm)
		"14" + "005000",                   // torque target (50.00 Nm)
		"15" + "005012",                   // torque actual (50.12 Nm)
		"16" + "00080",                    // angle min (80 deg)
		"17" + "00100",                    // angle max (100 deg)
		"18" + "00090",                    // angle target (90 deg)
		"19" + "00092",                    // angle actual (92 deg)
		"20" + "2026-06-01:12:00:00",      // timestamp (19 chars)
		"21" + "2026-06-01:11:00:00",      // pset change timestamp (19 chars)
		"22" + "0",                        // batch status
		"23" + "0007",                     // tightening id
	}
	return strings.Join(fields, "")
}

func padRight(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	return s + strings.Repeat(" ", n-len(s))
}

var _ = Describe("MID parsing", func() {

	Describe("ParseTelegram", func() {
		It("splits a frame into header and data field", func() {
			frame := op.BuildMessage(61, 1, []byte("PAYLOAD"))
			frame = frame[:len(frame)-1] // FrameReader strips the NUL
			tel, err := op.ParseTelegram(frame)
			Expect(err).NotTo(HaveOccurred())
			Expect(tel.Header.MID).To(Equal(61))
			Expect(string(tel.Data)).To(Equal("PAYLOAD"))
		})
	})

	Describe("MID 0061 last tightening (native parse)", func() {
		It("parses all typed fields from an OK result", func() {
			data := build0061Data(true)
			frame := op.BuildMessage(61, 1, []byte(data))
			frame = frame[:len(frame)-1]
			tel, err := op.ParseTelegram(frame)
			Expect(err).NotTo(HaveOccurred())

			lt, err := op.ParseLastTightening(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.CellID).To(Equal(1))
			Expect(lt.ChannelID).To(Equal(1))
			Expect(strings.TrimSpace(lt.ControllerName)).To(Equal("UMHTestSim"))
			Expect(strings.TrimSpace(lt.VIN)).To(Equal("VIN123"))
			Expect(lt.PsetNumber).To(Equal(1))
			Expect(lt.BatchSize).To(Equal(5))
			Expect(lt.BatchCounter).To(Equal(2))
			Expect(lt.TighteningOK).To(BeTrue())
			Expect(lt.TorqueTarget).To(BeNumerically("~", 50.00, 0.001))
			Expect(lt.TorqueActual).To(BeNumerically("~", 50.12, 0.001))
			Expect(lt.TorqueMin).To(BeNumerically("~", 47.00, 0.001))
			Expect(lt.TorqueMax).To(BeNumerically("~", 53.00, 0.001))
			Expect(lt.AngleTarget).To(Equal(90))
			Expect(lt.AngleActual).To(Equal(92))
			Expect(lt.Timestamp).To(Equal("2026-06-01:12:00:00"))
			Expect(lt.TighteningID).To(Equal(7))
		})

		It("parses a spec-compliant 10-digit tightening ID (R2.16 Table 98)", func() {
			// Real hardware sends a 10-digit tightening ID; the emulator sends 4.
			// The terminal field must accept both.
			data := build0061Data(true)
			data = data[:len(data)-len("0007")] + "4294967295" // replace 4-digit id with 10-digit
			frame := op.BuildMessage(61, 1, []byte(data))
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)
			lt, err := op.ParseLastTightening(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningID).To(Equal(4294967295))
		})

		It("reports NOK when the tightening status is 0", func() {
			data := build0061Data(false)
			frame := op.BuildMessage(61, 1, []byte(data))
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)
			lt, err := op.ParseLastTightening(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(lt.TighteningOK).To(BeFalse())
		})

		It("errors on a truncated data field", func() {
			frame := op.BuildMessage(61, 1, []byte("0100")) // only a partial first field
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)
			_, err := op.ParseLastTightening(tel)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("MID 0002 communication start acknowledge", func() {
		It("parses cell id, channel id and controller name", func() {
			data := "01" + "0001" + "02" + "01" + "03" + padRight("UMHTestSim", 25)
			frame := op.BuildMessage(2, 1, []byte(data))
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)
			cs, err := op.ParseCommunicationStart(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(cs.CellID).To(Equal(1))
			Expect(cs.ChannelID).To(Equal(1))
			Expect(strings.TrimSpace(cs.ControllerName)).To(Equal("UMHTestSim"))
		})
	})

	Describe("MID 0004 command error", func() {
		It("parses the failed MID and error code", func() {
			frame := op.BuildMessage(4, 1, []byte("000197")) // MID 0001 failed, error 97
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)
			ce, err := op.ParseCommandError(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(ce.FailedMID).To(Equal(1))
			Expect(ce.ErrorCode).To(Equal(97))
		})
	})

	Describe("Reassembler (multi-part telegrams)", func() {
		It("passes a single-part telegram straight through", func() {
			frame := op.BuildMessage(61, 1, []byte("ONEPART"))
			frame = frame[:len(frame)-1]
			tel, _ := op.ParseTelegram(frame)

			rs := op.NewReassembler()
			out, complete, err := rs.Push(tel)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(string(out.Data)).To(Equal("ONEPART"))
		})

		It("reassembles a two-part telegram into one payload", func() {
			rs := op.NewReassembler()

			p1 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 2, PartNumber: 1},
				Data:   []byte("FIRST-"),
			}
			out, complete, err := rs.Push(p1)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeFalse()) // waiting for part 2

			p2 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 2, PartNumber: 2},
				Data:   []byte("SECOND"),
			}
			out, complete, err = rs.Push(p2)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(string(out.Data)).To(Equal("FIRST-SECOND"))
		})

		It("errors when parts arrive out of order", func() {
			rs := op.NewReassembler()
			p2 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 2, PartNumber: 2},
				Data:   []byte("SECOND"),
			}
			_, _, err := rs.Push(p2)
			Expect(err).To(HaveOccurred())
		})

		It("rejects a sequence exceeding maxParts", func() {
			rs := op.NewReassembler()
			// A header claiming 17 parts (> maxParts=16), part 1.
			h := op.Header{MID: 9000, TotalParts: 17, PartNumber: 1}
			_, ok, err := rs.Push(op.Telegram{Header: h, Data: []byte("x")})
			Expect(ok).To(BeFalse())
			Expect(err).To(HaveOccurred())
		})

		It("rejects when assembled size would exceed maxAssembledBytes", func() {
			rs := op.NewReassembler()
			big := make([]byte, 40*1024) // 40 KiB per part
			h1 := op.Header{MID: 9001, TotalParts: 3, PartNumber: 1}
			_, _, err := rs.Push(op.Telegram{Header: h1, Data: big})
			Expect(err).NotTo(HaveOccurred())
			h2 := op.Header{MID: 9001, TotalParts: 3, PartNumber: 2}
			_, _, err = rs.Push(op.Telegram{Header: h2, Data: big}) // 80 KiB > 64 KiB
			Expect(err).To(HaveOccurred())
		})

		// TEST 3 — state discarded after out-of-order error, fresh sequence succeeds.
		//
		// A mutant that removes delete(rs.inflight, mid) on the out-of-order path
		// would leave the poisoned partialTelegram in place; the fresh part-1 would
		// then be rejected as "already in progress" or produce wrong output.
		It("discards inflight state after an out-of-order error, then accepts a fresh sequence", func() {
			rs := op.NewReassembler()

			// Step 1: push a valid part 1 of 3.
			p1 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 3, PartNumber: 1},
				Data:   []byte("PART-ONE"),
			}
			_, complete, err := rs.Push(p1)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeFalse())

			// Step 2: push part 3, skipping part 2 → out-of-order error.
			p3 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 3, PartNumber: 3},
				Data:   []byte("PART-THREE"),
			}
			_, _, err = rs.Push(p3)
			Expect(err).To(HaveOccurred(), "out-of-order part should return an error")

			// Step 3: push a FRESH part 1 of 2 for the same MID.
			fresh1 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 2, PartNumber: 1},
				Data:   []byte("FRESH-A"),
			}
			_, complete, err = rs.Push(fresh1)
			Expect(err).NotTo(HaveOccurred(), "fresh sequence should be accepted after error cleared state")
			Expect(complete).To(BeFalse())

			// Step 4: push part 2 of 2 → sequence completes.
			fresh2 := op.Telegram{
				Header: op.Header{MID: 61, TotalParts: 2, PartNumber: 2},
				Data:   []byte("-FRESH-B"),
			}
			out, complete, err := rs.Push(fresh2)
			Expect(err).NotTo(HaveOccurred())
			Expect(complete).To(BeTrue())
			Expect(string(out.Data)).To(Equal("FRESH-A-FRESH-B"))
		})

		// TEST 4 — maxAssembledBytes guard on the FIRST part alone.
		//
		// This exercises the part-1 size guard (distinct from the cumulative-append
		// guard tested above, which only fires on part 2+).
		It("rejects a part-1 whose data alone exceeds maxAssembledBytes (65 KiB)", func() {
			rs := op.NewReassembler()
			// 65 KiB > maxAssembledBytes (64 KiB).
			big := make([]byte, 65*1024)
			h1 := op.Header{MID: 9002, TotalParts: 2, PartNumber: 1}
			_, _, err := rs.Push(op.Telegram{Header: h1, Data: big})
			Expect(err).To(HaveOccurred(), "part-1 exceeding maxAssembledBytes should be rejected immediately")
		})

		// TEST 5 (FIX 1) — maxInflightMIDs cap on concurrent partial sequences.
		//
		// A buggy controller that starts a large number of distinct multi-part
		// sequences without completing any of them must not exhaust memory. The
		// Reassembler refuses to start a new sequence once the inflight map is full,
		// while existing in-progress sequences are unaffected.
		It("rejects a new sequence once maxInflightMIDs concurrent partials are tracked", func() {
			rs := op.NewReassembler()
			// Start part-1-of-2 for maxInflightMIDs distinct MIDs (8000, 8001, ...).
			// All must succeed (ok=false, err=nil) since each is a new partial.
			for i := 0; i < op.MaxInflightMIDs; i++ {
				mid := 8000 + i
				h := op.Header{MID: mid, TotalParts: 2, PartNumber: 1}
				_, complete, err := rs.Push(op.Telegram{Header: h, Data: []byte("x")})
				Expect(err).NotTo(HaveOccurred(), "part-1 for MID %d should be accepted (slot %d)", mid, i)
				Expect(complete).To(BeFalse())
			}
			// One more distinct MID — the map is full, must be rejected.
			overflowMID := 8000 + op.MaxInflightMIDs
			h := op.Header{MID: overflowMID, TotalParts: 2, PartNumber: 1}
			_, _, err := rs.Push(op.Telegram{Header: h, Data: []byte("x")})
			Expect(err).To(HaveOccurred(), "starting a new sequence when inflight map is full should return an error")
		})
	})

})
