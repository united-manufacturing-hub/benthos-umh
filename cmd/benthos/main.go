package main

import (
	"github.com/Jeffail/benthos/v3/lib/service"
	_ "github.com/makenew/benthos-plugin/v1/plugin"
)

func main() {
	service.Run()
}
