package plugin

import (
	"github.com/Jeffail/benthos/v3/public/bloblang"
)

func init() {
	bloblang.RegisterMethod("todo", func(args ...interface{}) (bloblang.Method, error) {
		return func(v interface{}) (interface{}, error) {
			return v, nil
		}, nil
	})
}
