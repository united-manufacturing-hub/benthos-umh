package schemavalidation

import (
	"fmt"
	"sync"

	"github.com/kaptinlin/jsonschema"
)

var schemaCompiler = jsonschema.NewCompiler()
var schemaCompilerMutex sync.Mutex

type SchemaVersion struct {
	JSONSchema *jsonschema.Schema
}

type SchemaVersions struct {
	Versions map[uint64]SchemaVersion
}

type Schema struct {
	Name     string
	Versions SchemaVersions
}

func NewSchema(name string) *Schema {
	return &Schema{
		Name:     name,
		Versions: SchemaVersions{Versions: make(map[uint64]SchemaVersion)},
	}
}

func (s *Schema) AddVersion(version uint64, schema []byte) error {
	schemaCompilerMutex.Lock()
	defer schemaCompilerMutex.Unlock()

	compiledSchema, err := schemaCompiler.Compile(schema)
	if err != nil {
		return fmt.Errorf("error compiling schema: %v", err)
	}

	s.Versions.Versions[version] = SchemaVersion{
		JSONSchema: compiledSchema,
	}
	return nil
}

func (s *Schema) HasVersion(version uint64) bool {
	_, ok := s.Versions.Versions[version]
	if !ok {
		return false
	}

	if s.Versions.Versions[version].JSONSchema == nil {
		return false
	}
	return ok
}

func (s *Schema) GetVersion(version uint64) *jsonschema.Schema {
	return s.Versions.Versions[version].JSONSchema
}
