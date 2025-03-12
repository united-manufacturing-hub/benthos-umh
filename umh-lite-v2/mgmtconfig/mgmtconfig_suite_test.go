package mgmtconfig_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"

	"github.com/united-manufacturing-hub/ManagementConsole/shared/tools"
)

func TestMgmtconfig(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Mgmtconfig Suite")
}

var _ = BeforeSuite(func() {
	By("initializing logging")
	tools.InitLogging()

})
