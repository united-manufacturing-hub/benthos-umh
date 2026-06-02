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
	"bytes"
	"io"
	"net"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	op "github.com/united-manufacturing-hub/benthos-umh/open_protocol_plugin"
)

var _ = Describe("TCP framer", func() {

	// A couple of canonical, complete telegrams (header + NUL).
	login := op.BuildMessage(1, 1, nil)               // MID 0001, len 20
	result := op.BuildMessage(61, 1, []byte("HELLO")) // MID 0061, len 25

	It("reads a single complete frame and strips the NUL terminator", func() {
		fr := op.NewFrameReader(bytes.NewReader(login))
		frame, err := fr.ReadFrame()
		Expect(err).NotTo(HaveOccurred())
		Expect(frame).To(Equal(login[:len(login)-1])) // without NUL
		Expect(frame).NotTo(ContainElement(byte(0x00)))
	})

	It("reads multiple back-to-back frames from one buffer", func() {
		stream := append(append([]byte{}, login...), result...)
		fr := op.NewFrameReader(bytes.NewReader(stream))

		f1, err := fr.ReadFrame()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(f1[4:8])).To(Equal("0001"))

		f2, err := fr.ReadFrame()
		Expect(err).NotTo(HaveOccurred())
		Expect(string(f2[4:8])).To(Equal("0061"))
		Expect(string(f2[20:25])).To(Equal("HELLO"))

		_, err = fr.ReadFrame()
		Expect(err).To(Equal(io.EOF))
	})

	It("reassembles a frame delivered across multiple partial writes", func() {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		fr := op.NewFrameReader(client)

		// Writer splits the telegram mid-header and mid-data.
		go func() {
			defer GinkgoRecover()
			_, _ = server.Write(result[0:2]) // partial length field
			time.Sleep(5 * time.Millisecond)
			_, _ = server.Write(result[2:12]) // rest of length + part of header
			time.Sleep(5 * time.Millisecond)
			_, _ = server.Write(result[12:]) // remainder incl. NUL
		}()

		frame, err := fr.ReadFrame()
		Expect(err).NotTo(HaveOccurred())
		Expect(frame).To(Equal(result[:len(result)-1]))
	})

	It("returns an error on a non-numeric length field", func() {
		fr := op.NewFrameReader(bytes.NewReader([]byte("XXXX0001001000000    \x00")))
		_, err := fr.ReadFrame()
		Expect(err).To(HaveOccurred())
	})

	It("returns an error when the NUL terminator is missing", func() {
		// 21 bytes that look like a length-20 telegram but the 21st byte is not NUL.
		bad := append([]byte{}, login...)
		bad[len(bad)-1] = 'X' // corrupt the terminator
		fr := op.NewFrameReader(bytes.NewReader(bad))
		_, err := fr.ReadFrame()
		Expect(err).To(HaveOccurred())
	})

	It("propagates EOF when the stream ends cleanly between frames", func() {
		fr := op.NewFrameReader(bytes.NewReader(login))
		_, err := fr.ReadFrame()
		Expect(err).NotTo(HaveOccurred())
		_, err = fr.ReadFrame()
		Expect(err).To(Equal(io.EOF))
	})

	// TEST 5 — framer length lower-bound guard (Edge #2).
	//
	// HeaderLength == 20. A length field that is a valid 4-digit number but < 20
	// must be rejected as out-of-range. This exercises the
	//   if length < HeaderLength || length > maxFrameLength
	// branch in ReadFrame. A mutant that removes the lower-bound comparison
	// (e.g. changes it to length > maxFrameLength) would pass 0019 or 0010 as
	// valid, and these tests would fail.
	It("rejects a length field of '0019' (< HeaderLength=20, numeric but out of range)", func() {
		// Build a stream whose 4-byte length prefix decodes to 19.
		// The remaining bytes are filler; ReadFrame should error before consuming them.
		buf := append([]byte("0019"), bytes.Repeat([]byte("X"), 20)...)
		buf = append(buf, 0x00) // NUL terminator (should never be reached)
		fr := op.NewFrameReader(bytes.NewReader(buf))
		_, err := fr.ReadFrame()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("out of range"))
	})

	It("rejects a length field of '0010' (< HeaderLength=20, numeric but out of range)", func() {
		buf := append([]byte("0010"), bytes.Repeat([]byte("Y"), 20)...)
		buf = append(buf, 0x00)
		fr := op.NewFrameReader(bytes.NewReader(buf))
		_, err := fr.ReadFrame()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("out of range"))
	})
})
