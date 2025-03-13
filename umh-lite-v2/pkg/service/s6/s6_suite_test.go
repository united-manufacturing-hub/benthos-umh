package s6

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestS6Service(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S6 Service Suite")
}
