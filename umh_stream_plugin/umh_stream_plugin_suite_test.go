package umhstreamplugin

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestUmhStreamPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "UmhStreamPlugin Suite")
}
