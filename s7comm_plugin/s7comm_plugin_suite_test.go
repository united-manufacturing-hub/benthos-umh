package s7comm_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestS7commPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S7commPlugin Suite")
}
