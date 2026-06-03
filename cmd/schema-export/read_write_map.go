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
	"maps"
	"sort"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// pluginPair maps an input plugin to its corresponding write-flow output plugin.
type pluginPair struct {
	Read        string `json:"read"`
	Write       string `json:"write"`
	IsUmhPlugin bool   `json:"umh"`
}

// overrides for input -> output plugins, which are meant to be paired, but don't have matching names.
var readWriteOverrides = []pluginPair{
	// stdin -> stdout
	{
		Read:  "stdin",
		Write: "stdout",
	},
	// gcp_bigquery_select -> gcp_bigquery
	{
		Read:  "gcp_bigquery_select",
		Write: "gcp_bigquery",
	},
	// sql_select -> sql_insert
	{
		Read:  "sql_select",
		Write: "sql_insert",
	},
	// aws_dynamodb_cdc -> aws_dynamodb
	{
		Read:  "aws_dynamodb_cdc",
		Write: "aws_dynamodb",
	},
}

// buildMapping derives the read->write pairing from all registered inputs and outputs, applies overrides, and returns it sorted by plugin name.
func buildMapping(env *service.Environment, overrides []pluginPair) []pluginPair {
	registeredInputs := make(map[string]bool)

	// get non-deprecated inputs from redpanda-connect and benthos-umh
	env.WalkInputs(func(name string, view *service.ConfigView) {
		if view.IsDeprecated() {
			return
		}
		registeredInputs[name] = true
	})

	// get non-deprecated outputs from redpanda-connect and benthos-umh
	registeredOutputs := make(map[string]bool)
	env.WalkOutputs(func(name string, view *service.ConfigView) {
		if view.IsDeprecated() {
			return
		}
		registeredOutputs[name] = true
	})

	overrideMap := walkOverrides(overrides)

	// merge maps together with deduplication
	names := make(map[string]bool, len(registeredInputs)+len(registeredOutputs))
	maps.Copy(names, registeredInputs)
	maps.Copy(names, registeredOutputs)

	pairs := make([]pluginPair, 0, len(names)+len(overrides))
	pairs = append(pairs, overrides...)

	// overrides get skipped entirely, rest gets paired
	for name := range names {
		if overrideMap[name] {
			continue
		}
		pair := pair(name, registeredInputs, registeredOutputs)
		pairs = append(pairs, pair)
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairKey(pairs[i]) < pairKey(pairs[j])
	})

	return pairs
}

// pair pairs a plugin name with itself on each side it is registered for; unregistered sides stay empty.
func pair(name string, inputs, outputs map[string]bool) pluginPair {
	p := pluginPair{IsUmhPlugin: umhPluginNames[name]}
	if inputs[name] {
		p.Read = name
	}
	if outputs[name] {
		p.Write = name
	}
	return p
}

// walkOverrides returns every read and write name consumed by an override.
func walkOverrides(overrides []pluginPair) map[string]bool {
	names := make(map[string]bool)
	for _, p := range overrides {
		if p.Read != "" {
			names[p.Read] = true
		}
		if p.Write != "" {
			names[p.Write] = true
		}
	}
	return names
}

// pairKey is the plugin name a pair was derived from (read side wins).
func pairKey(p pluginPair) string {
	if p.Read != "" {
		return p.Read
	}
	return p.Write
}
