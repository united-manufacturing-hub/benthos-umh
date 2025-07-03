package schemavalidation

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/united-manufacturing-hub/benthos-umh/pkg/umh/topic"
)

var _ = Describe("Validator", func() {
	Context("when validating basic data", func() {

		schema := []byte(`
		{
   "type": "object",
   "properties": {
		"virtual_path": {
          "type": "string",
          "enum": ["vibration.x-axis"]
       },
      "fields": {
         "type": "object",
         "properties": {
            "value": {
               "type": "object",
               "properties": {
                  "timestamp_ms": {"type": "number"},
                  "value": {"type": "number"}
               },
               "required": ["timestamp_ms", "value"],
               "additionalProperties": false
            }
         },
         "additionalProperties": false
      }
   },
   "required": ["virtual_path", "fields"],
   "additionalProperties": false
}`)
		var validator *Validator

		BeforeEach(func() {
			validator = NewValidator()
			validator.LoadSchema("_sensor_data", 1, schema)
			Expect(validator.HasSchema("_sensor_data", 1)).To(BeTrue())
		})

		/*
			Test cases:
			1. Valid data & valid virtual_path
			2. Valid data & invalid virtual_path
			3. Invalid data & valid virtual_path
			4. Invalid data & invalid virtual_path
		*/

		It("should pass validation for valid data & valid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.x-axis")
			Expect(err).To(BeNil())
			err = validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(err).To(BeNil())
		})

		It("should fail validation for valid data & invalid virtual_path", func() {
			topic, err := topic.NewUnsTopic("umh.v1.enterprise.site.area._sensor_data-v1.vibration.non-existing")
			Expect(err).To(BeNil())
			err = validator.Validate(topic, []byte(`{"value": {"timestamp_ms": 1719859200000, "value": 100}}`))
			Expect(err).To(Not(BeNil()))
		})
	})
})
