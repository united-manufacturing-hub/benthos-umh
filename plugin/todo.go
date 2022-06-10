package plugin

import (
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	todoSpec := bloblang.NewPluginSpec()

	err := bloblang.RegisterMethodV2("todo", todoSpec, func(args *bloblang.ParsedParams) (bloblang.Method, error) {
		return func(v interface{}) (interface{}, error) {
			return v, nil
		}, nil
	})
	if err != nil {
		panic(err)
	}
}
