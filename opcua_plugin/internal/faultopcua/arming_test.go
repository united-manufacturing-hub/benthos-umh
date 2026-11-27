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

package faultopcua_test

import (
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gopcua/opcua/ua"

	"github.com/united-manufacturing-hub/benthos-umh/opcua_plugin/internal/faultopcua"
)

// armCaseEnv selects one arm case in a child run of this same test binary.
// The arm methods refuse what they cannot arm by failing the test, and a
// refusal nobody can observe is not a refusal, so each case runs in a
// subprocess and the parent asserts the child died with the refusal message.
const armCaseEnv = "FAULTOPCUA_ARM_CASE"

// TestArming_RefusesWhatItCannotArm pins the guards that replaced the two
// zero-value traps:
//
//   - RefuseWrite (and RefuseRead, which shares the parameter) armed without
//     a status used to answer Good -- the machine's honest answer -- while
//     recording a firing and reporting fired.
//   - RaiseFlagLate with no wait used to be over before it was armed: it
//     never took effect and never recorded anything, and read exactly like a
//     fault that never injected.
//
// An arm method cannot be called without the value it needs, and cannot be
// called with a value that makes the fault meaningless.
func TestArming_RefusesWhatItCannotArm(t *testing.T) {
	cases := []struct {
		name string
		want string // a fragment of the refusal message, not the method name:
		//      the fallback "was accepted" message names the method too, and
		//      a guard removed by a mutation must not slip past on it
		run func(t *testing.T)
	}{
		{"RefuseWrite with the zero status", "Good status, which means success", func(t *testing.T) {
			srv := faultopcua.New(t)
			v := srv.AddVariable("Setpoint", int32(7))
			srv.RefuseWrite(v, ua.StatusOK)
		}},
		{"RefuseWrite with the other Good status", "Good status, which means success", func(t *testing.T) {
			srv := faultopcua.New(t)
			v := srv.AddVariable("Setpoint", int32(7))
			srv.RefuseWrite(v, ua.StatusGood)
		}},
		{"RefuseRead with a Good status", "Good status, which means success", func(t *testing.T) {
			srv := faultopcua.New(t)
			v := srv.AddVariable("Flag", true)
			srv.RefuseRead(v, ua.StatusOK)
		}},
		{"RaiseFlagLate with no wait at all", "after must be positive", func(t *testing.T) {
			srv := faultopcua.New(t)
			v := srv.AddVariable("Loaded", true)
			srv.RaiseFlagLate(v, false, 0)
		}},
		{"RaiseFlagLate with a negative wait", "after must be positive", func(t *testing.T) {
			srv := faultopcua.New(t)
			v := srv.AddVariable("Loaded", true)
			srv.RaiseFlagLate(v, false, -time.Second)
		}},
	}

	if child, ok := os.LookupEnv(armCaseEnv); ok {
		for _, tc := range cases {
			if tc.name != child {
				continue
			}
			tc.run(t)
			// Only reached if the arm method returned, which it must not.
			t.Fatalf("%s was accepted", child)
			return
		}
		t.Fatalf("unknown arm case %q", child)
		return
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run", "^TestArming_RefusesWhatItCannotArm$", "-test.count=1")
			cmd.Env = append(os.Environ(), armCaseEnv+"="+tc.name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("arming was accepted: the child ran to completion; output:\n%s", out)
			}
			if !strings.Contains(string(out), tc.want) {
				t.Errorf("the child failed (%v) but not by refusing; output:\n%s", err, out)
			}
		})
	}
}
