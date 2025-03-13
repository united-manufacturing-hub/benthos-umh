package main

import (
	"context"
	"log"

	"github.com/united-manufacturing-hub/benthos-umh/umh-lite-v2/pkg/control"
)

func main() {
	log.Println("Starting umh-lite-v2...")

	controlLoop := control.NewControlLoop()
	controlLoop.Execute(context.Background())

	log.Println("umh-lite-v2 test completed")
}
