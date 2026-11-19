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

// Command docs-gen writes the pages for upstream Redpanda Connect inputs and
// outputs under docs/, from the component specs of the version benthos-umh
// is built with. components.yaml lists the components that get a page.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/redpanda-data/benthos/v4/public/service"
	"gopkg.in/yaml.v3"

	// Only the upstream components: UMH plugins have hand-written pages.
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"
)

type allowlist struct {
	Inputs  []entry `yaml:"inputs"`
	Outputs []entry `yaml:"outputs"`
}

type entry struct {
	Name  string   `yaml:"name"`
	Title string   `yaml:"title"`
	Hide  []string `yaml:"hide"`
}

func (a allowlist) entries(s section) []entry {
	if s.TypeDir == "inputs" {
		return a.Inputs
	}
	return a.Outputs
}

func main() {
	docsDir := flag.String("docs", "docs", "docs directory to write into")
	genDir := flag.String("gen", "cmd/docs-gen", "directory holding components.yaml and overlays/")
	flag.Parse()

	if err := run(service.GlobalEnvironment(), *docsDir, *genDir, connectVersion()); err != nil {
		fmt.Fprintf(os.Stderr, "docs-gen: %v\n", err)
		os.Exit(1)
	}
}

func run(env *service.Environment, docsDir, genDir, version string) error {
	raw, err := os.ReadFile(filepath.Join(genDir, "components.yaml"))
	if err != nil {
		return err
	}
	var list allowlist
	if err := yaml.Unmarshal(raw, &list); err != nil {
		return fmt.Errorf("parsing components.yaml: %w", err)
	}

	schema, err := loadSchema(env, version)
	if err != nil {
		return err
	}

	files, err := generate(schema, list, version, func(s section, name string) (string, error) {
		b, err := os.ReadFile(filepath.Join(genDir, "overlays", s.TypeDir, name+".md"))
		if errors.Is(err, fs.ErrNotExist) {
			return "", nil
		}
		return string(b), err
	})
	if err != nil {
		return err
	}

	summaryPath := filepath.Join(docsDir, "SUMMARY.md")
	summary, err := os.ReadFile(summaryPath)
	if err != nil {
		return err
	}
	updated := string(summary)
	for _, s := range sections {
		if updated, err = updateSummary(updated, s, list.entries(s)); err != nil {
			return err
		}
		// The generated directories hold nothing else, so clearing them
		// removes pages of components dropped from components.yaml.
		if err := os.RemoveAll(filepath.Join(docsDir, s.pageDir())); err != nil {
			return err
		}
	}
	for path, content := range files {
		full := filepath.Join(docsDir, path)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(full, content, 0o644); err != nil {
			return err
		}
	}
	if err := os.WriteFile(summaryPath, []byte(updated), 0o644); err != nil {
		return err
	}
	fmt.Printf("docs-gen: wrote %d pages from Redpanda Connect %s\n", len(files), version)
	return nil
}

// generate renders every page, keyed by its path relative to docs/.
func generate(schema *schemaDoc, list allowlist, version string, overlay func(section, string) (string, error)) (map[string][]byte, error) {
	documented := map[string]bool{}
	for _, s := range sections {
		for _, e := range list.entries(s) {
			documented[s.TypeDir+"/"+e.Name] = true
		}
	}

	files := map[string][]byte{}
	for _, s := range sections {
		all := schema.Inputs
		if s.TypeDir == "outputs" {
			all = schema.Outputs
		}
		byName := map[string]component{}
		for _, c := range all {
			byName[c.Name] = c
		}

		entries := list.entries(s)
		rw := linkRewriter{from: s, documented: documented}
		for _, e := range entries {
			c, ok := byName[e.Name]
			if !ok {
				return nil, fmt.Errorf("components.yaml lists %s %q, which Redpanda Connect %s does not include", s.Singular, e.Name, version)
			}
			if e.Title == "" {
				return nil, fmt.Errorf("components.yaml: %s %q has no title", s.Singular, e.Name)
			}
			ov, err := overlay(s, e.Name)
			if err != nil {
				return nil, err
			}
			page, err := renderPage(s, c, e, ov, version, rw)
			if err != nil {
				return nil, err
			}
			files[s.pageDir()+"/"+e.Name+".md"] = page
		}

		catalog, err := renderCatalog(s, all, entries, version)
		if err != nil {
			return nil, err
		}
		files[s.pageDir()+"/README.md"] = catalog
	}
	return files, nil
}
