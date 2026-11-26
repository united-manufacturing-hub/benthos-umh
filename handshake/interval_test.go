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

package handshake

import (
	"strings"
	"testing"
	"time"
)

// An hour is not a long handshake, it is a mistake. These pin both places a
// too-long interval can enter: the config, refused when the pipeline loads,
// and a wait_until's own argument, refused when the script asks for it.
//
// The config half matters most. A budget this long only ever shows itself
// as a bridge that reports healthy and delivers nothing, an hour at a time,
// and config time is the only moment the mistake is still cheap.

func TestValidate_RefusesABudgetLongerThanAnHour(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"the script's own deadline", Config{
			Script:           `return opcua.success({});`,
			MaxScriptRuntime: time.Hour + time.Second,
		}},
		{"an onSuccess budget", Config{
			Script:              `return opcua.success({});`,
			MaxScriptRuntime:    5 * time.Second,
			OnSuccess:           `opcua.write({ nodeId: "ns=2;s=R" }, false);`,
			MaxOnSuccessRuntime: 2 * time.Hour,
		}},
		{"an onError budget", Config{
			Script:            `return opcua.success({});`,
			MaxScriptRuntime:  5 * time.Second,
			OnError:           `opcua.write({ nodeId: "ns=2;s=R" }, false);`,
			MaxOnErrorRuntime: 90 * time.Minute,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatal("a budget longer than an hour was accepted")
			}
			if !strings.Contains(err.Error(), "longer than the") {
				t.Errorf("refused for the wrong reason: %v", err)
			}
		})
	}
}

func TestValidate_AcceptsAnHourExactly(t *testing.T) {
	cfg := Config{Script: `return opcua.success({});`, MaxScriptRuntime: time.Hour}
	if err := cfg.Validate(); err != nil {
		t.Errorf("exactly the limit should be allowed, got %v", err)
	}
}
