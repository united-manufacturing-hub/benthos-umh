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

package main

import (
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// pluginPair maps an input plugin to its corresponding write-flow output plugin.
type pluginPair struct {
	Input  string `json:"input"`
	Output string `json:"output"`
}

//nolint:all
var inputOutputOverrides = []pluginPair{}

// buildMapping derives the input->output pairing, applies overrides, and returns it sorted by input name.
func buildMapping(env *service.Environment, overrides []pluginPair) []pluginPair {
	registeredOutputs := make(map[string]bool)
	env.WalkOutputs(func(name string, _ *service.ConfigView) {
		registeredOutputs[name] = true
	})

	overrideByInput := make(map[string]string)
	for _, p := range overrides {
		overrideByInput[p.Input] = p.Output
	}

	var pairs []pluginPair
	env.WalkInputs(func(name string, _ *service.ConfigView) {
		output := ""
		if registeredOutputs[name] {
			output = name
		}
		o, ok := overrideByInput[name]
		if ok {
			output = o
		}
		pairs = append(pairs, pluginPair{Input: name, Output: output})
	})

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Input < pairs[j].Input
	})
	return pairs
}
