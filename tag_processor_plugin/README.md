
Overview

The tagProcessor is a Benthos processor plugin designed to:
	1.	Set up a canonical metadata structure (msg.meta) for constructing a standardized topic and payload schema.
	2.	Define a series of defaults and conditional transformations that adapt metadata fields based on incoming message metadata (e.g., opcua_node_id, folder).
	3.	Optionally apply advanced transformations to the payload after all metadata processing is complete.

The processor aims to combine clarity and structure with the flexibility familiar to Node-RED function nodes, by re-using the nodered_js processor’s JavaScript execution environment. The tagProcessor uses a three-stage approach:
	1.	Defaults: A single JavaScript code snippet to set static baseline values.
	2.	Conditions: A list of conditions, each with an if (JavaScript expression) and a then (JavaScript snippet) section.
	3.	Advanced Processing: An optional JavaScript snippet to modify the payload after all conditions have been applied.

The final output message will have a constructed Kafka topic and a standardized payload conforming to the UMH schema conventions.

Required Fields and Resulting Topic

The following metadata fields must be set by the end of processing:
	•	msg.meta.level0 (e.g., enterprise)
	•	msg.meta.level1 (optional, e.g., site)
	•	msg.meta.level2 (optional, e.g., area)
	•	msg.meta.level3 (optional)
	•	msg.meta.level4 (optional)
	•	msg.meta.schema (e.g., _historian)
	•	msg.meta.folder (optional grouping, e.g., OEE or folderA)
	•	msg.meta.tagName (measurement name)

These fields combine to form a Kafka topic of the format:

umh.v1.<level0>.<level1>.<level2>.<level3>.<level4>.<schema>.<folder>.<tagName>

Empty or undefined levels are skipped, and consecutive dots are normalized to a single dot. Trailing or leading dots are trimmed.

The payload structure is as follows:

{
  "<tagName>": <msg.payload>,
  "timestamp_ms": <timestamp>
}

The timestamp_ms is a Unix timestamp in milliseconds representing the message processing time.

Configuration

The processor is configured as follows:

tagProcessor:
  defaults: |
      // JavaScript code (Node-RED style) to set initial values
      // No conditional logic is allowed here by recommendation (not enforced)
      msg.meta.level0 = "MyEnterprise";
      msg.meta.level1 = "MySite";
      msg.meta.level2 = "MyArea";
      msg.meta.level3 = "MyLine";
      msg.meta.level4 = "MyWorkCell";
      msg.meta.schema = "_historian";
      msg.meta.folder = "MyFolder";
      msg.meta.tagName = "MyTagName";
      return msg;

  conditions:
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "temperature";
    - if: msg.meta.opcua_node_id === "ns=1;i=2246"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "humidity";
    - if: msg.meta.folder.startsWith("OEE")
      then: |
        msg.meta.level2 = "OEEArea";    
    - if: msg.meta.folder.includes("folderA")
      then: |
        msg.meta.tagName = msg.meta.opcua_tag_name || "defaultTagFromFolderA";

  advancedProcessing: |
      // Optional advanced logic, runs last
      if (typeof msg.payload.value === 'number') {
        msg.payload.value *= 2;
      }
      return msg;

Sections

	1.	defaults:
	•	Executed first.
	•	Should contain only static assignments to msg.meta.
	•	Returns msg at the end.
	•	No conditions recommended here—this is to keep the logic organized.
	2.	conditions:
	•	A list of condition objects, each with an if and a then.
	•	if: A JavaScript expression evaluated against msg.
	•	then: A JavaScript snippet that runs if if is true.
	•	No return statements in then sections; the plugin handles message returning internally.
	•	Conditions are evaluated top-to-bottom, and multiple conditions can apply if multiple if statements are true.
	3.	advancedProcessing:
	•	An optional JavaScript block that runs after all conditions have been applied.
	•	This can contain more complex transformations of msg.payload or msg.meta.
	•	Must return msg at the end.

Execution Flow

	1.	Convert Benthos Message to Node-RED Style:
Internally, the tagProcessor will convert the Benthos message into the Node-RED style JavaScript environment used by the nodered_js processor:
	•	msg.payload for content
	•	msg.meta for metadata
	2.	Run Defaults Code:
	•	Executes the defaults code once.
	•	Must return msg.
	3.	Evaluate Each Condition:
	•	For each condition:
	•	Evaluate the if expression.
	•	If true, run the then code snippet.
	•	No need to return msg here; msg is implicitly passed through.
	4.	Run Advanced Processing (Optional):
	•	If present, execute advancedProcessing code.
	•	Must return msg.
	5.	Reconstruct Final Message:
	•	Construct the Kafka topic from msg.meta fields.
	•	Build the payload object: { "<tagName>": <msg.payload>, "timestamp_ms": <timestamp> }.
	•	Convert back into Benthos message format with updated content and metadata.

Reuse of nodered_js Plugin

To avoid code duplication and maintain familiarity:
	•	JavaScript Execution:
Leverage the existing nodered_js processor’s JavaScript runtime. The tagProcessor should internally use the same execution environment, functions, and message conversion routines provided by nodered_js.
	•	Message Conversion:
The tagProcessor will wrap calls to the nodered_js runner. For each code snippet (defaults, each condition’s then, and advanced processing):
	•	Convert Benthos message to nodered_js format (msg.payload, msg.meta).
	•	Run the given JavaScript code.
	•	Convert msg back into a Benthos message.
By using nodered_js capabilities, we ensure consistent behavior, logging, and error handling.
	•	Error Handling and Logging:
Any errors or exceptions that occur during script execution should use the same error handling mechanism as nodered_js. This ensures consistent debugging and observability.

Validation and Error Handling

	•	If required metadata fields are missing by the end of processing, the processor can:
	•	Log a warning or error.
	•	Optionally add a configurable option to drop or re-route invalid messages.
	•	If a JavaScript syntax error or runtime error occurs in defaults, conditions or advancedProcessing code, the message should be handled according to nodered_js error handling logic (e.g., messages_errored metric increments, message dropped or forwarded to a dead-letter output if configured).

Metrics

	•	messages_processed: Count of messages successfully processed.
	•	messages_errored: Count of messages that caused JavaScript errors.
	•	messages_dropped: Count of messages dropped due to conditions or errors.
	•	Additional metrics can be inherited from nodered_js if available.

Conclusion

This RFC provides a full specification for a tagProcessor Benthos plugin that:
	•	Sets up standard metadata fields.
	•	Applies configurable defaults and conditional overrides using a Node-RED–style JS environment.
	•	Optionally performs advanced payload transformations.
	•	Integrates tightly with the existing nodered_js processor to minimize duplication and maintain consistency in code execution and error handling.

Given this specification, an LLM or a developer can implement tagProcessor by primarily leveraging the nodered_js environment and adding logic for defaults, condition evaluation, advanced processing, and final topic/payload construction.



## This is how it should look like to the user
```yaml
tag_processor:
  # Required metadata fields:
  # - msg.meta.level0 (e.g., enterprise)
  # - msg.meta.level1 (optional, e.g., site)
  # - msg.meta.level2 (optional, e.g., area)
  # - msg.meta.level3 (optional)
  # - msg.meta.level4 (optional)
  # - msg.meta.schema (e.g., "_historian")
  # - msg.meta.folder (optional, e.g., "OEE" or "folderA", think of it as a sub-group)
  # - msg.meta.tagName (the actual measurement name)

  # The final topic: umh.v1.<level0>.<level1>.<level2>.<level3>.<level4>.<schema>.<folder>.<tagName>
  # The payload will have:
  # {
  #   "<tagName>": <msg.payload>,   // e.g., {"temperature": 23.5}
  #   "timestamp_ms": <timestamp>
  # }

  # Set basic defaults (change as needed)
  defaults: |
      msg.meta.level0 = "MyEnterprise";
      msg.meta.level1 = "MySite";
      msg.meta.level2 = "MyArea";
      msg.meta.level3 = "MyLine";
      msg.meta.level4 = "MyWorkCell";
      msg.meta.schema = "_historian";
      msg.meta.folder = "MyFolder";
      msg.meta.tagName = "MyTagName";
      return msg;

  # All conditions are checked from top to bottom, and multiple conditions can be true
  conditions:

    # If the OPC UA node_id matches 2245, adjust these values
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "temperature";

    - if: msg.meta.opcua_node_id === "ns=1;i=2246"
      then: |
        msg.meta.level2 = "SpecialArea";
        msg.meta.tagName = "humidity";

    # If the folder starts with "OEE", set level2 accordingly
    - if: msg.meta.folder.startsWith("OEE")
      then: |
        msg.meta.level2 = "OEEArea";    

    # If the folder starts with "folderA", set tagName from opcua_tag_name if available
    - if: msg.meta.folder.includes("folderA")
      then: |
        msg.meta.tagName = msg.meta.opcua_tag_name || "defaultTagFromFolderA";

  # Advanced processing is executed after all conditions have been processed
  advancedProcessing: |
      // Optional more advanced logic
      // Example: double a numeric value
      if (typeof msg.payload.value === 'number') {
        msg.payload.value *= 2;
      }
      return msg;
```
