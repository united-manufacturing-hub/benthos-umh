package adsPlugin_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAdsPlugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AdsPlugin Suite")
}
