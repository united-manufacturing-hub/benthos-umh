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

// CommandError is the decoded payload of a MID 0004 "Command error" telegram:
// the controller's negative reply to a client request. Its data field is a
// fixed 6-character field: the 4-digit MID that failed followed by a 2-digit
// error code (e.g. 0001 + 96 = "client already connected", 0001 + 97 =
// "MID revision unsupported").
type CommandError struct {
	FailedMID int
	ErrorCode int
}

// ParseCommandError decodes a MID 0004 telegram.
func ParseCommandError(t Telegram) (CommandError, error) {
	if t.Header.MID != MIDCommandError {
		return CommandError{}, fmt.Errorf("open protocol: ParseCommandError called on MID %04d", t.Header.MID)
	}
	if len(t.Data) < 6 {
		return CommandError{}, fmt.Errorf("open protocol: MID 0004 data too short: %d bytes", len(t.Data))
	}
	return CommandError{
		FailedMID: atoiTrim(string(t.Data[0:4])),
		ErrorCode: atoiTrim(string(t.Data[4:6])),
	}, nil
}

// Error renders a CommandError as a Go error message.
func (e CommandError) Error() string {
	return fmt.Sprintf("open protocol command error: MID %04d failed with error code %02d", e.FailedMID, e.ErrorCode)
}
