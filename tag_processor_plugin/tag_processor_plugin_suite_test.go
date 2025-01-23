package tag_processor_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestTagProcessorPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "TagProcessor Plugin Suite")
}
