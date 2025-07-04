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

// Package schemavalidation provides JSON schema validation functionality for UNS topics.
// It manages schema compilation, versioning, and validation operations with thread-safe access.
package schemavalidation

import (
	"fmt"
	"sync"

	"github.com/kaptinlin/jsonschema"
)

// Global schema compiler and mutex for thread-safe schema compilation.
// These are shared across all schema instances to optimize compilation performance.
var (
	schemaCompiler      = jsonschema.NewCompiler()
	schemaCompilerMutex sync.RWMutex
)

// SchemaVersion represents a compiled JSON schema for a specific version.
type SchemaVersion struct {
	JSONSchema *jsonschema.Schema
}

// SchemaVersions manages multiple versions of a schema.
type SchemaVersions struct {
	Versions map[uint64]SchemaVersion
}

// Schema represents a named schema with multiple versions.
type Schema struct {
	Name     string
	Versions SchemaVersions
}

// NewSchema creates a new Schema with the given name and initializes an empty version map.
func NewSchema(name string) *Schema {
	return &Schema{
		Name:     name,
		Versions: SchemaVersions{Versions: make(map[uint64]SchemaVersion)},
	}
}

// AddVersion compiles and adds a new schema version to this Schema.
// It returns an error if schema compilation fails.
func (s *Schema) AddVersion(version uint64, schema []byte) error {
	schemaCompilerMutex.Lock()
	defer schemaCompilerMutex.Unlock()

	compiledSchema, err := schemaCompiler.Compile(schema)
	if err != nil {
		return fmt.Errorf("failed to compile schema for version %d: %w", version, err)
	}

	s.Versions.Versions[version] = SchemaVersion{
		JSONSchema: compiledSchema,
	}
	return nil
}

// HasVersion checks if a specific version exists and has a valid compiled schema.
func (s *Schema) HasVersion(version uint64) bool {
	schemaCompilerMutex.RLock()
	defer schemaCompilerMutex.RUnlock()

	schemaVersion, exists := s.Versions.Versions[version]
	return exists && schemaVersion.JSONSchema != nil
}

// GetVersion retrieves the compiled JSON schema for the specified version.
// Returns nil if the version doesn't exist.
func (s *Schema) GetVersion(version uint64) *jsonschema.Schema {
	schemaCompilerMutex.RLock()
	defer schemaCompilerMutex.RUnlock()

	if schemaVersion, exists := s.Versions.Versions[version]; exists {
		return schemaVersion.JSONSchema
	}
	return nil
}
