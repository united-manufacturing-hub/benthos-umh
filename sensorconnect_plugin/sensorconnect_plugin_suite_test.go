package sensorconnect_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSensorconnectPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SensorconnectPlugin Suite")
}
