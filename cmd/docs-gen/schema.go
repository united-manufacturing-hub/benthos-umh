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
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// connectModule is the module whose version the generated pages name.
const connectModule = "github.com/redpanda-data/connect/v4"

// upstreamDocsURL is where links in upstream doc strings point, and the base
// for links to components that have no page on docs.umh.app.
const upstreamDocsURL = "https://docs.redpanda.com/redpanda-connect"

// schemaDoc is the part of the Benthos JSON schema (`benthos list --format
// json-full`) that the generator reads.
type schemaDoc struct {
	Inputs  []component `json:"inputs"`
	Outputs []component `json:"outputs"`
}

type component struct {
	Name        string             `json:"name"`
	Type        string             `json:"type"`
	Status      string             `json:"status"`
	Summary     string             `json:"summary"`
	Description string             `json:"description"`
	Footnotes   string             `json:"footnotes"`
	Categories  []string           `json:"categories"`
	Examples    []componentExample `json:"examples"`
	Config      field              `json:"config"`
}

type componentExample struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Config  string `json:"config"`
}

type field struct {
	Name             string          `json:"name"`
	Type             string          `json:"type"`
	Kind             string          `json:"kind"`
	Description      string          `json:"description"`
	IsAdvanced       bool            `json:"is_advanced"`
	IsOptional       bool            `json:"is_optional"`
	IsDeprecated     bool            `json:"is_deprecated"`
	IsSecret         bool            `json:"is_secret"`
	Interpolated     bool            `json:"interpolated"`
	Bloblang         bool            `json:"bloblang"`
	Default          json.RawMessage `json:"default"`
	Examples         []any           `json:"examples"`
	Options          []string        `json:"options"`
	AnnotatedOptions [][2]string     `json:"annotated_options"`
	Children         []field         `json:"children"`
}

func (f field) hasDefault() bool { return len(f.Default) > 0 }

// loadSchema renders the schema of every component registered in env, with
// doc strings converted from AsciiDoc to Markdown.
func loadSchema(env *service.Environment, version string) (*schemaDoc, error) {
	raw, err := env.FullConfigSchema(version, "").MarshalJSONV0Markdown(service.DocsMarkdownOptions{
		DocumentationURL: upstreamDocsURL,
	})
	if err != nil {
		return nil, fmt.Errorf("rendering schema: %w", err)
	}
	var doc schemaDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("parsing schema: %w", err)
	}
	return &doc, nil
}

// connectVersion returns the Redpanda Connect version this binary is built
// with, such as v4.117.0.
func connectVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == connectModule {
				if dep.Replace != nil {
					return dep.Replace.Version
				}
				return dep.Version
			}
		}
	}
	return "unknown"
}
