package modbus_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestModbusPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ModbusPlugin Suite")
}
