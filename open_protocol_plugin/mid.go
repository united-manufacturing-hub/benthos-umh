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

// MID numbers used by this plugin. Open Protocol assigns every message type a
// 4-digit MID; these are the ones the session layer sends or specially handles.
const (
	MIDCommunicationStart    = 1    // client -> controller: login
	MIDCommunicationStartAck = 2    // controller -> client: login accepted
	MIDCommunicationStop     = 3    // client -> controller: logout
	MIDCommandError          = 4    // controller -> client: command error
	MIDCommandAccepted       = 5    // controller -> client: command accepted
	MIDLastTighteningSub     = 60   // client -> controller: subscribe last tightening
	MIDLastTightening        = 61   // controller -> client: tightening result (pushed)
	MIDLastTighteningAck     = 62   // client -> controller: acknowledge a result
	MIDLastTighteningUnsub   = 63   // client -> controller: unsubscribe
	MIDAlarmSub              = 70   // client -> controller: subscribe alarms
	MIDAlarm                 = 71   // controller -> client: alarm (pushed)
	MIDAlarmAck              = 72   // client -> controller: acknowledge an alarm
	MIDAlarmUnsub            = 73   // client -> controller: unsubscribe alarms
	MIDGenericSubData        = 6    // client -> controller: generic data request/subscribe
	MIDGenericSubscribe      = 8    // client -> controller: generic subscribe
	MIDGenericUnsubscribe    = 9    // client -> controller: generic unsubscribe
	MIDKeepAlive             = 9999 // bidirectional: keep-alive
)

// Telegram is a fully-received (and, for multi-part messages, reassembled)
// Open Protocol message: the parsed header plus the raw data field (the bytes
// following the 20-byte header, with the NUL terminator already stripped by
// the FrameReader).
type Telegram struct {
	Header Header
	Data   []byte
}

// ParseTelegram parses a single complete frame (as returned by
// FrameReader.ReadFrame) into a Telegram. It does not perform multi-part
// reassembly; feed Telegrams through a Reassembler for that.
func ParseTelegram(frame []byte) (Telegram, error) {
	h, err := ParseHeader(frame)
	if err != nil {
		return Telegram{}, err
	}
	data := frame[HeaderLength:]
	return Telegram{Header: h, Data: data}, nil
}

// pidField describes one parameter-ID field in a parameter-ID-formatted data
// field: a 2-digit id followed by a fixed-width ASCII value.
type pidField struct {
	id    string
	width int
}

// widthRest marks a pidField whose value extends to the end of the data field.
// It is only valid for the final field in a layout (e.g. MID 0061's tightening
// ID, whose width differs between spec-compliant hardware and some emulators).
const widthRest = -1

// scanPIDFields walks a parameter-ID-formatted data field, validating that the
// expected parameter ids appear in order and extracting each fixed-width value.
// A field with width widthRest consumes the remaining bytes.
func scanPIDFields(data []byte, fields []pidField) (map[string]string, error) {
	out := make(map[string]string, len(fields))
	pos := 0
	for _, f := range fields {
		if pos+2 > len(data) {
			return nil, fmt.Errorf("open protocol: truncated before parameter %s", f.id)
		}
		gotID := string(data[pos : pos+2])
		if gotID != f.id {
			return nil, fmt.Errorf("open protocol: expected parameter %s, got %q at offset %d", f.id, gotID, pos)
		}
		pos += 2

		if f.width == widthRest {
			out[f.id] = string(data[pos:])
			pos = len(data)
			continue
		}
		if pos+f.width > len(data) {
			return nil, fmt.Errorf("open protocol: truncated reading parameter %s", f.id)
		}
		out[f.id] = string(data[pos : pos+f.width])
		pos += f.width
	}
	return out, nil
}

func atoiTrim(s string) int {
	v, _ := strconv.Atoi(strings.TrimSpace(s))
	return v
}

// Bounds on multi-part reassembly to prevent unbounded memory growth from a
// buggy or malicious controller.
const (
	maxParts          = 16
	maxAssembledBytes = 64 * 1024
)

// Reassembler joins the parts of multi-part Open Protocol telegrams back into a
// single Telegram. Open Protocol splits a telegram that exceeds the maximum
// length into N parts carrying the same MID, with the header's "number of
// messages" and "message number" fields indicating N and the part index. Parts
// for at most one MID at a time are tracked, which matches how controllers
// stream them.
type Reassembler struct {
	inflight map[int]*partialTelegram
}

type partialTelegram struct {
	total   int
	nextIdx int
	header  Header
	buf     []byte
}

// NewReassembler returns an empty Reassembler.
func NewReassembler() *Reassembler {
	return &Reassembler{inflight: make(map[int]*partialTelegram)}
}

// Push feeds the next received Telegram into the reassembler. When the Telegram
// is single-part (or completes a multi-part sequence) it returns the assembled
// Telegram and complete=true; otherwise complete=false and the part is buffered
// until its successors arrive.
func (rs *Reassembler) Push(t Telegram) (Telegram, bool, error) {
	if t.Header.TotalParts <= 1 {
		return t, true, nil
	}

	mid := t.Header.MID

	if t.Header.TotalParts > maxParts {
		return Telegram{}, false, fmt.Errorf("open protocol: MID %04d declares %d parts, exceeds max %d", mid, t.Header.TotalParts, maxParts)
	}

	p, ok := rs.inflight[mid]
	if !ok {
		if t.Header.PartNumber != 1 {
			return Telegram{}, false, fmt.Errorf("open protocol: MID %04d multi-part started at part %d, expected 1", mid, t.Header.PartNumber)
		}
		if len(t.Data) > maxAssembledBytes {
			return Telegram{}, false, fmt.Errorf("open protocol: MID %04d part 1 data (%d bytes) exceeds max assembled size %d", mid, len(t.Data), maxAssembledBytes)
		}
		rs.inflight[mid] = &partialTelegram{
			total:   t.Header.TotalParts,
			nextIdx: 2,
			header:  t.Header,
			buf:     append([]byte{}, t.Data...),
		}
		return Telegram{}, false, nil
	}

	if t.Header.PartNumber != p.nextIdx {
		delete(rs.inflight, mid)
		return Telegram{}, false, fmt.Errorf("open protocol: MID %04d out-of-order part %d, expected %d", mid, t.Header.PartNumber, p.nextIdx)
	}

	if len(p.buf)+len(t.Data) > maxAssembledBytes {
		delete(rs.inflight, mid)
		return Telegram{}, false, fmt.Errorf("open protocol: MID %04d assembled size would exceed max %d bytes", mid, maxAssembledBytes)
	}

	p.buf = append(p.buf, t.Data...)
	p.nextIdx++

	if t.Header.PartNumber == p.total {
		delete(rs.inflight, mid)
		h := p.header
		h.TotalParts = 1
		h.PartNumber = 1
		return Telegram{Header: h, Data: p.buf}, true, nil
	}

	return Telegram{}, false, nil
}
