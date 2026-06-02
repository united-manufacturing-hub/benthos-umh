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
	"strings"
)

// LastTightening is the natively-decoded payload of a MID 0061 "Last tightening
// result data" telegram (revision 1). It is the headline event for OEE/quality:
// one tightening (screw-driving) operation with its torque/angle measurements
// and OK/NOK verdict.
//
// The data field uses the Open Protocol parameter-ID format: each value is
// preceded by a 2-digit parameter id. Field widths follow the Atlas Copco Open
// Protocol specification, revision 1 (verified against R2.16 Table 98 and
// cross-checked against the Apache-licensed node-nutrunner-open-library and the
// reference emulator). Torque values are transmitted as hundredths of the
// engineering unit (Nm) and are scaled back to a float here; angles are whole
// degrees.
type LastTightening struct {
	CellID         int     `json:"cell_id"`
	ChannelID      int     `json:"channel_id"`
	ControllerName string  `json:"controller_name"`
	VIN            string  `json:"vin"`
	JobID          int     `json:"job_id"`
	PsetNumber     int     `json:"pset_number"`
	BatchSize      int     `json:"batch_size"`
	BatchCounter   int     `json:"batch_counter"`
	TighteningOK   bool    `json:"tightening_ok"` // parameter 09: true when status == 1 (OK)
	TorqueStatus   int     `json:"torque_status"` // parameter 10: 0 = low, 1 = OK, 2 = high
	AngleStatus    int     `json:"angle_status"`  // parameter 11: 0 = low, 1 = OK, 2 = high
	TorqueMin      float64 `json:"torque_min"`
	TorqueMax      float64 `json:"torque_max"`
	TorqueTarget   float64 `json:"torque_target"`
	TorqueActual   float64 `json:"torque_actual"`
	AngleMin       int     `json:"angle_min"`
	AngleMax       int     `json:"angle_max"`
	AngleTarget    int     `json:"angle_target"`
	AngleActual    int     `json:"angle_actual"`
	Timestamp      string  `json:"timestamp"`        // result timestamp, format YYYY-MM-DD:HH:MM:SS
	PsetChangeTime string  `json:"pset_change_time"` // last Pset-change timestamp, same format
	BatchStatus    int     `json:"batch_status"`     // parameter 22: 0 = not complete, 1 = complete
	TighteningID   int     `json:"tightening_id"`
}

// mid0061Fields is the ordered parameter-ID layout of a MID 0061 revision-1
// data field. Widths are fixed by the specification.
var mid0061Fields = []pidField{
	{"01", 4},         // cell id
	{"02", 2},         // channel id
	{"03", 25},        // controller name
	{"04", 25},        // VIN
	{"05", 2},         // job id
	{"06", 3},         // pset number
	{"07", 4},         // batch size
	{"08", 4},         // batch counter
	{"09", 1},         // tightening status
	{"10", 1},         // torque status
	{"11", 1},         // angle status
	{"12", 6},         // torque min (x100)
	{"13", 6},         // torque max (x100)
	{"14", 6},         // torque target (x100)
	{"15", 6},         // torque actual (x100)
	{"16", 5},         // angle min
	{"17", 5},         // angle max
	{"18", 5},         // angle target
	{"19", 5},         // angle actual
	{"20", 19},        // timestamp
	{"21", 19},        // pset change timestamp
	{"22", 1},         // batch status
	{"23", widthRest}, // tightening id: 10 digits per spec (R2.16 Table 98), but
	//                    some emulators send 4; as the final field it greedily
	//                    consumes the remainder, handling both.
}

// ParseLastTightening decodes a MID 0061 telegram into a LastTightening.
func ParseLastTightening(t Telegram) (LastTightening, error) {
	if t.Header.MID != MIDLastTightening {
		return LastTightening{}, fmt.Errorf("open protocol: ParseLastTightening called on MID %04d", t.Header.MID)
	}

	f, err := scanPIDFields(t.Data, mid0061Fields)
	if err != nil {
		return LastTightening{}, err
	}

	return LastTightening{
		CellID:         atoiTrim(f["01"]),
		ChannelID:      atoiTrim(f["02"]),
		ControllerName: strings.TrimSpace(f["03"]),
		VIN:            strings.TrimSpace(f["04"]),
		JobID:          atoiTrim(f["05"]),
		PsetNumber:     atoiTrim(f["06"]),
		BatchSize:      atoiTrim(f["07"]),
		BatchCounter:   atoiTrim(f["08"]),
		TighteningOK:   atoiTrim(f["09"]) == 1,
		TorqueStatus:   atoiTrim(f["10"]),
		AngleStatus:    atoiTrim(f["11"]),
		TorqueMin:      float64(atoiTrim(f["12"])) / 100.0,
		TorqueMax:      float64(atoiTrim(f["13"])) / 100.0,
		TorqueTarget:   float64(atoiTrim(f["14"])) / 100.0,
		TorqueActual:   float64(atoiTrim(f["15"])) / 100.0,
		AngleMin:       atoiTrim(f["16"]),
		AngleMax:       atoiTrim(f["17"]),
		AngleTarget:    atoiTrim(f["18"]),
		AngleActual:    atoiTrim(f["19"]),
		Timestamp:      f["20"],
		PsetChangeTime: f["21"],
		BatchStatus:    atoiTrim(f["22"]),
		TighteningID:   atoiTrim(f["23"]),
	}, nil
}
