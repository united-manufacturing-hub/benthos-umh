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

import "fmt"

// CommunicationStart is the decoded payload of a MID 0002 "Communication start
// acknowledge" telegram: the controller's positive reply to a login (MID 0001).
// It identifies the cell, channel and controller the session is attached to.
type CommunicationStart struct {
	CellID         int
	ChannelID      int
	ControllerName string
}

// mid0002Fields is the ordered parameter-ID layout of a MID 0002 revision-1
// data field.
var mid0002Fields = []pidField{
	{"01", 4},  // cell id
	{"02", 2},  // channel id
	{"03", 25}, // controller name
}

// ParseCommunicationStart decodes a MID 0002 telegram. The controller name is
// returned space-padded as sent on the wire; callers that want it trimmed
// should trim it themselves.
func ParseCommunicationStart(t Telegram) (CommunicationStart, error) {
	if t.Header.MID != MIDCommunicationStartAck {
		return CommunicationStart{}, fmt.Errorf("open protocol: ParseCommunicationStart called on MID %04d", t.Header.MID)
	}
	f, err := scanPIDFields(t.Data, mid0002Fields)
	if err != nil {
		return CommunicationStart{}, err
	}
	return CommunicationStart{
		CellID:         atoiTrim(f["01"]),
		ChannelID:      atoiTrim(f["02"]),
		ControllerName: f["03"],
	}, nil
}
