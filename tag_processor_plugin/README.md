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
  # - msg.meta.virtualPath (optional, e.g., "OEE.production.line1", a logical non-physical grouping)
  # - msg.meta.tagName (the actual measurement name)

  # The final topic / fulltagname: umh.v1.<level0>.<level1>.<level2>.<level3>.<level4>.<schema>.<virtualPath>.<tagName>
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
      msg.meta.virtualPath = "OEE.production.line1";  // Logical grouping with nested path
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

    # If the virtualPath starts with "OEE", set level2 accordingly
    - if: msg.meta.virtualPath.startsWith("OEE")
      then: |
        msg.meta.level2 = "OEEArea";    

    # If the virtualPath includes "production", set tagName from opcua_tag_name if available
    - if: msg.meta.virtualPath.includes("production")
      then: |
        msg.meta.tagName = msg.meta.opcua_tag_name || "defaultTagFromProduction";

  # Advanced processing is executed after all conditions have been processed
  advancedProcessing: |
      // Optional more advanced logic
      // Example: double a numeric value
      msg.payload.value *= 2;

      msg.meta.level0 = "enterprise";
      msg.meta.level1 = "site";
      msg.meta.level2 = "area";
      msg.meta.level3 = "line";
      msg.meta.level4 = "workcell";
      msg.meta.schema = "_analytics";
      msg.meta.virtualPath = "work_order";  // Logical grouping
      msg.payload = {
        "work_order_id": msg.payload.work_order_id,
        "work_order_start_time": umh.getHistorianValue("enterprise.site.area.line.workcell._historian.workorder.work_order_start_time"),
        "work_order_end_time": umh.getHistorianValue("enterprise.site.area.line.workcell._historian.workorder.work_order_end_time")
      };
      return msg;
```

