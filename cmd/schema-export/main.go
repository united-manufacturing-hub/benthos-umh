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
	"flag"
	"fmt"
	"os"
	"strings"
)

// generateVersionedFilename creates a versioned filename by stripping 'v' prefix
func generateVersionedFilename(version string) string {
	cleanVersion := strings.TrimPrefix(version, "v")
	return fmt.Sprintf("benthos-schemas-v%s.json", cleanVersion)
}

func main() {
	version := flag.String("version", "", "Benthos-UMH version (required)")
	flag.Parse()

	if *version == "" {
		fmt.Fprintf(os.Stderr, "Error: -version flag is required\n")
		fmt.Fprintf(os.Stderr, "Usage: schema-export -version 0.11.6\n")
		os.Exit(1)
	}

	// Add path traversal protection
	if strings.ContainsAny(*version, "/\\") {
		fmt.Fprintf(os.Stderr, "Error: -version contains invalid path characters\n")
		fmt.Fprintf(os.Stderr, "Version should be a semantic version like 0.11.6\n")
		os.Exit(1)
	}

	outputFile := generateVersionedFilename(*version)

	schemas, err := generateSchemas()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error generating schemas: %v\n", err)
		os.Exit(1)
	}

	data, err := json.MarshalIndent(schemas, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outputFile, data, 0644); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing output: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Generated schemas to %s\n", outputFile)
	fmt.Printf("   - %d inputs\n", len(schemas.Inputs))
	fmt.Printf("   - %d processors\n", len(schemas.Processors))
	fmt.Printf("   - %d outputs\n", len(schemas.Outputs))
}
