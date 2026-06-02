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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("Header codec", func() {
	Describe("ParseHeader", func() {
		It("parses a well-formed 20-byte header (login accept MID 0002)", func() {
			// 0020 0002 001 0 00 00 + 4 spare spaces = 20-byte header, empty data.
			// Replace the visible placeholder with the canonical 4 spare spaces.
			raw := []byte("0020000200100" + "00" + "00" + "    ")

			h, err := op.ParseHeader(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.Length).To(Equal(20))
			Expect(h.MID).To(Equal(2))
			Expect(h.Revision).To(Equal(1))
			Expect(h.NoAck).To(BeFalse())
			Expect(h.StationID).To(Equal(0))
			Expect(h.SpindleID).To(Equal(0))
			Expect(h.Sequence).To(Equal(0))
			Expect(h.TotalParts).To(Equal(1))
			Expect(h.PartNumber).To(Equal(1))
		})

		It("treats a blank (all-spaces) revision as revision 1", func() {
			raw := []byte("00200002" + "   " + "0" + "00" + "00" + "    ")
			h, err := op.ParseHeader(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.Revision).To(Equal(1))
		})

		It("parses revision 0 as revision 1 (spec: 000 means latest/1)", func() {
			raw := []byte("00200002" + "000" + "0" + "00" + "00" + "    ")
			h, err := op.ParseHeader(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.Revision).To(Equal(1))
		})

		It("parses the no-ack flag", func() {
			raw := []byte("00200061" + "001" + "1" + "00" + "00" + "    ")
			h, err := op.ParseHeader(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.NoAck).To(BeTrue())
		})

		It("parses station, spindle, sequence and multi-part fields", func() {
			// station 02, spindle 05, sequence 07, 3 total parts, part 2.
			raw := []byte("00610061" + "006" + "0" + "02" + "05" + "07" + "3" + "2")
			h, err := op.ParseHeader(raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.StationID).To(Equal(2))
			Expect(h.SpindleID).To(Equal(5))
			Expect(h.Sequence).To(Equal(7))
			Expect(h.TotalParts).To(Equal(3))
			Expect(h.PartNumber).To(Equal(2))
		})

		It("rejects a buffer shorter than the 20-byte header", func() {
			_, err := op.ParseHeader([]byte("0020000200"))
			Expect(err).To(HaveOccurred())
		})

		It("rejects a non-numeric length", func() {
			raw := []byte("XXXX0002" + "001" + "0" + "00" + "00" + "    ")
			_, err := op.ParseHeader(raw)
			Expect(err).To(HaveOccurred())
		})

		It("rejects a non-numeric MID", func() {
			raw := []byte("0020" + "ABCD" + "001" + "0" + "00" + "00" + "    ")
			_, err := op.ParseHeader(raw)
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("BuildMessage", func() {
		It("builds a login telegram (MID 0001) with length header and NUL terminator", func() {
			msg := op.BuildMessage(1, 1, nil)
			// 4 len + 4 mid + 3 rev + 1 ack + 2 station + 2 spindle + 4 spare = 20, then NUL.
			Expect(msg).To(HaveLen(21))
			Expect(msg[len(msg)-1]).To(Equal(byte(0x00)))
			Expect(string(msg[0:4])).To(Equal("0020"))
			Expect(string(msg[4:8])).To(Equal("0001"))
			Expect(string(msg[8:11])).To(Equal("001"))
			Expect(msg[11]).To(Equal(byte('0'))) // ack required
		})

		It("builds a telegram carrying a data field and computes the length", func() {
			msg := op.BuildMessage(60, 1, []byte("DATA"))
			// length = 20 header + 4 data = 24.
			Expect(string(msg[0:4])).To(Equal("0024"))
			Expect(string(msg[4:8])).To(Equal("0060"))
			Expect(string(msg[20:24])).To(Equal("DATA"))
			Expect(msg[len(msg)-1]).To(Equal(byte(0x00)))
		})

		It("round-trips through ParseHeader", func() {
			msg := op.BuildMessage(9999, 1, nil)
			h, err := op.ParseHeader(msg)
			Expect(err).NotTo(HaveOccurred())
			Expect(h.MID).To(Equal(9999))
			Expect(h.Length).To(Equal(20))
			Expect(h.Revision).To(Equal(1))
		})

		// FIX 5 — BuildMessage panics when data would cause telegram length to exceed
		// the 4-digit ASCII length field maximum (9999 bytes).
		It("panics when data would make the telegram exceed maxFrameLength", func() {
			// HeaderLength=20, so 9980 bytes of data → length 10000 > 9999.
			Expect(func() { op.BuildMessage(1, 1, make([]byte, 9980)) }).To(Panic())
		})
	})
})
