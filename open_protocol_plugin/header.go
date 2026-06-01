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
	"fmt"
	"strconv"
	"strings"
)

// HeaderLength is the fixed size, in bytes, of every Open Protocol telegram
// header. All header fields are ASCII and occupy fixed positions:
//
//	offset  width  field
//	0       4      Length (total telegram length, excluding the NUL terminator)
//	4       4      MID (message ID)
//	8       3      Revision ("001".."999"; 3 spaces or "000" mean revision 1)
//	11      1      No-ack flag ("1" = no ack expected, otherwise ack required)
//	12      2      Station ID
//	14      2      Spindle ID
//	16      2      Sequence number (link-layer; blank when unused)
//	18      1      Number of messages (total parts for a split telegram)
//	19      1      Message number (this part's index, 1-based)
//
// The data field (if any) follows the header and the whole telegram is
// terminated by a single NUL (0x00) byte.
const HeaderLength = 20

// Header holds the parsed fields of an Open Protocol telegram header.
type Header struct {
	Length     int
	MID        int
	Revision   int
	NoAck      bool
	StationID  int
	SpindleID  int
	Sequence   int
	TotalParts int // total number of parts in a multi-part telegram (>=1)
	PartNumber int // 1-based index of this part (>=1)
}

// ParseHeader parses the fixed 20-byte ASCII header at the start of buf. buf
// may contain more than just the header (e.g. the data field and terminator);
// only the first HeaderLength bytes are inspected.
func ParseHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderLength {
		return Header{}, fmt.Errorf("open protocol: header too short: got %d bytes, need %d", len(buf), HeaderLength)
	}

	var h Header
	var err error

	if h.Length, err = parseNumField(buf[0:4], "length"); err != nil {
		return Header{}, err
	}
	if h.MID, err = parseNumField(buf[4:8], "MID"); err != nil {
		return Header{}, err
	}

	// Revision: 3 spaces or "000" both denote revision 1.
	rev := strings.TrimSpace(string(buf[8:11]))
	if rev == "" {
		h.Revision = 1
	} else {
		if h.Revision, err = parseNumField(buf[8:11], "revision"); err != nil {
			return Header{}, err
		}
		if h.Revision == 0 {
			h.Revision = 1
		}
	}

	h.NoAck = buf[11] == '1'

	// Station and spindle default to 0 when left blank.
	h.StationID = parseNumFieldDefault(buf[12:14], 0)
	h.SpindleID = parseNumFieldDefault(buf[14:16], 0)
	h.Sequence = parseNumFieldDefault(buf[16:18], 0)

	// Multi-part fields: blank or "0" means a single, complete telegram.
	h.TotalParts = parseNumFieldDefault(buf[18:19], 0)
	if h.TotalParts == 0 {
		h.TotalParts = 1
	}
	h.PartNumber = parseNumFieldDefault(buf[19:20], 0)
	if h.PartNumber == 0 {
		h.PartNumber = 1
	}

	return h, nil
}

// BuildMessage constructs a complete Open Protocol telegram (header + data +
// NUL terminator) for transmission to a controller. Station, spindle, sequence
// and the multi-part fields are left blank/zero, which is correct for all
// client-initiated requests this plugin sends (login, subscribe, ack,
// keep-alive, etc.). The no-ack flag is left unset (ack required).
func BuildMessage(mid, revision int, data []byte) []byte {
	if revision <= 0 {
		revision = 1
	}

	length := HeaderLength + len(data)

	var b strings.Builder
	b.Grow(length + 1)
	fmt.Fprintf(&b, "%04d", length)   // length
	fmt.Fprintf(&b, "%04d", mid)      // MID
	fmt.Fprintf(&b, "%03d", revision) // revision
	b.WriteByte('0')                  // no-ack flag: ack required
	b.WriteString("00")               // station
	b.WriteString("00")               // spindle
	b.WriteString("    ")             // sequence + parts: blank (4 spaces)
	b.Write(data)                     // data field
	b.WriteByte(0x00)                 // NUL terminator

	return []byte(b.String())
}

func parseNumField(b []byte, name string) (int, error) {
	v, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("open protocol: invalid %s field %q: %w", name, string(b), err)
	}
	return v, nil
}

// parseNumFieldDefault parses an ASCII numeric field, returning def when the
// field is blank or not a valid number (used for optional/spare header fields).
func parseNumFieldDefault(b []byte, def int) int {
	s := strings.TrimSpace(string(b))
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}
