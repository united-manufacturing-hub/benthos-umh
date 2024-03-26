package opcua_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOpcuaPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpcuaPlugin Suite")
}
