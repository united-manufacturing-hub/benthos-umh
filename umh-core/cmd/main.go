package main

import (
	"context"
	"log"

	"github.com/united-manufacturing-hub/benthos-umh/umh-core/pkg/control"
)

func main() {
	log.Println("Starting umh-core...")

	controlLoop := control.NewControlLoop()
	controlLoop.Execute(context.Background())

	log.Println("umh-core test completed")
}
