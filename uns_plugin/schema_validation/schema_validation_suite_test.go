package schemavalidation_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSchemaValidation(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SchemaValidation Suite")
}
