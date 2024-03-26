package adsPlugin_test

import (
	"context"
	"os"
	"strconv"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ads_plugin "github.com/united-manufacturing-hub/benthos-umh/v2/ads_plugin"
)

var _ = Describe("AdsPlugin", func() {
	var input *ads_plugin.AdsCommInput

	var targetIP string
	var targetAMS string
	var targetPort int
	var runtimePort int
	var hostAMS string
	var hostPort int

	// Before suite can be started, check if the env variables are set
	BeforeEach(
		func() {
			var err error

			ip := os.Getenv("TEST_BECKHOFF_IP")
			port := os.Getenv("TEST_BECKHOFF_PORT")

			hostIPEnv := os.Getenv("TEST_HOST_IP")

			// Check if environment variables are set
			if ip == "" || port == "" {
				Skip("Skipping test: environment variables are not set")
				return
			}

			targetIP = ip
			targetAMS = ip + ".1.1"
			targetPort, err = strconv.Atoi(port)
			Expect(err).ToNot(HaveOccurred())

			runtimePort = 851

			hostAMS = hostIPEnv + ".1.1"
			hostPort = 851

		},
	)

	When("a connection to the beckhoff PLC is established", func() {

		BeforeEach(func() {
			symbols := []string{"MAIN.MYBOOL"}
			maxDelay := 100
			cycleTime := 100
			upperCase := true

			symbolList := ads_plugin.CreateSymbolList(symbols, maxDelay, cycleTime, upperCase)

			input = &ads_plugin.AdsCommInput{
				TargetIP:    targetIP,
				TargetAMS:   targetAMS,
				TargetPort:  targetPort,
				RuntimePort: runtimePort,
				HostAMS:     hostAMS,
				HostPort:    hostPort,
				Symbols:     symbolList,
				MaxDelay:    maxDelay,
				CycleTime:   cycleTime,
			}

		})

		It("should connect to the PLC", func() {

			err := input.Connect(context.Background())
			Expect(err).ToNot(HaveOccurred())

		})

		It("should be able to read from the PLC", func() {
		})
	})

})
