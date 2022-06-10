package main

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/service"
	_ "github.com/makenew/benthos-plugin/v2/plugin"
)

func main() {
	service.RunCLI(context.Background())
}
