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

package open_protocol_plugin

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// maxFrameLength is the largest telegram the 4-digit ASCII length field can
// describe. Anything outside [HeaderLength, 9999] is treated as a framing error.
const maxFrameLength = 9999

// FrameReader splits an Open Protocol byte stream into individual telegrams.
//
// Each telegram begins with a 4-digit ASCII length field giving the total
// telegram length (including those 4 bytes, but excluding the trailing NUL
// terminator). FrameReader reads exactly that many bytes, verifies the NUL
// terminator, and returns the telegram with the terminator stripped. Partial
// reads from the underlying stream are transparently buffered, so a single
// telegram split across several TCP segments is reassembled correctly.
type FrameReader struct {
	r *bufio.Reader
}

// NewFrameReader wraps r with buffering and returns a FrameReader.
func NewFrameReader(r io.Reader) *FrameReader {
	return &FrameReader{r: bufio.NewReader(r)}
}

// ReadFrame reads and returns the next complete telegram (header + data), with
// the trailing NUL terminator removed. It returns io.EOF when the stream ends
// cleanly on a frame boundary, and a non-EOF error on a malformed frame or a
// short read mid-frame.
func (fr *FrameReader) ReadFrame() ([]byte, error) {
	// The length field is the first 4 bytes of every telegram. A clean EOF
	// here (no bytes buffered) signals the end of the stream.
	lenField := make([]byte, 4)
	if _, err := io.ReadFull(fr.r, lenField); err != nil {
		// io.ReadFull reports io.EOF only when zero bytes were read; a partial
		// read becomes io.ErrUnexpectedEOF, which we surface as-is.
		return nil, err
	}

	length, err := strconv.Atoi(strings.TrimSpace(string(lenField)))
	if err != nil {
		return nil, fmt.Errorf("open protocol: invalid frame length %q: %w", string(lenField), err)
	}
	if length < HeaderLength || length > maxFrameLength {
		return nil, fmt.Errorf("open protocol: frame length %d out of range [%d, %d]", length, HeaderLength, maxFrameLength)
	}

	// Read the remainder of the telegram (everything after the length field).
	frame := make([]byte, length)
	copy(frame, lenField)
	if _, err := io.ReadFull(fr.r, frame[4:]); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}

	// Consume and validate the NUL terminator.
	term, err := fr.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return nil, err
	}
	if term != 0x00 {
		return nil, fmt.Errorf("open protocol: missing NUL terminator after %d-byte frame, got 0x%02x", length, term)
	}

	return frame, nil
}
