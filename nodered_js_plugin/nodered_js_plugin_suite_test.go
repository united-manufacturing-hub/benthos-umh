package nodered_js_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestNodeREDJSPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "NodeREDJS Plugin Suite")
}
