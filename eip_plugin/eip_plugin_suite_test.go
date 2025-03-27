package eip_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestEIPPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "EIPPlugin Suite")
}
