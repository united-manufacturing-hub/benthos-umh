package algorithms_test

import (
	"os"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAlgorithms(t *testing.T) {
	// Only run if TEST_DOWNSAMPLER is set
	if os.Getenv("TEST_DOWNSAMPLER") == "" {
		t.Skip("Skipping downsampler tests. Set TEST_DOWNSAMPLER=1 to run.")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Downsampler Algorithms Suite")
}
