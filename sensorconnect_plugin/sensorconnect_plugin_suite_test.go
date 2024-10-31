package sensorconnect_plugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSensorconnectPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	suiteConfig, _ := GinkgoConfiguration()
	suiteConfig.ParallelTotal = 1 // ensure it runs in serial
	RunSpecs(t, "SensorconnectPlugin Suite", suiteConfig)
}
