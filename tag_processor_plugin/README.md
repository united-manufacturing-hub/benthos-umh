## This is how it should look like to the user
```yaml
tag_processor:
  # Required metadata fields:
  # - msg.meta.location0 (e.g., enterprise)
  # - msg.meta.location1 (optional, e.g., site)
  # - msg.meta.location2 (optional, e.g., area)
  # - msg.meta.location3 (optional, e.g., line)
  # - msg.meta.location4 (optional, e.g., workcell)
  # - msg.meta.location5 (optional, e.g., originID)
  # - msg.meta.datacontract (e.g., "_historian")
  # - msg.meta.path0 (optional, e.g., "axis", a logical non-physical grouping)
  # - msg.meta.path1 (optional, e.g., "x", a logical non-physical grouping)
  # - msg.meta.path2 (optional, e.g., "position", a logical non-physical grouping)
  # - msg.meta.tagName (the actual measurement name)

  # The final topic / fulltagname: umh.v1.<location0>.<location1>.<location2>.<location3>.<location4>.<location5>.<datacontract>.<path0>.<path1>.<path2>....<pathN>.<tagName>
  # The payload will have:
  # {
  #   "<tagName>": <msg.payload>,   // e.g., {"temperature": 23.5}
  #   "timestamp_ms": <timestamp>
  # }

  # Set basic defaults (change as needed)
  defaults: |
      msg.meta.location0 = "enterprise";
      msg.meta.location1 = "plant1";
      msg.meta.location2 = "machiningArea";
      msg.meta.location3 = "cnc-line";
      msg.meta.location4 = "cnc5";
      msg.meta.location5 = "plc123";

      msg.meta.datacontract = "_historian";
      return msg;

  # All conditions are checked from top to bottom, and multiple conditions can be true
  conditions:
    # If the OPC UA node_id matches 2245, adjust these values
    - if: msg.meta.opcua_node_id === "ns=1;i=2245"
      then: |
        newmsg = msg;

        msg.meta.path0 = "axis";
        msg.meta.path1 = "x";
        msg.meta.path2 = "position";
        msg.meta.tagName = "value";

        return [newmsg, msg];


    - if: msg.meta.opcua_node_id === "ns=1;i=2246"
      then: |
        newmsg = msg;

        msg.meta.path0 = "axis";
        msg.meta.path1 = "y";
        msg.meta.path2 = "position";
        msg.meta.tagName = "value";

        return [newmsg, msg];


  # Advanced processing is executed after all conditions have been processed
  advancedProcessing: |
      // Optional more advanced logic
      // Example: double a numeric value
      msg.payload.actual *= 2;

      msg.meta.location0 = "enterprise";
      msg.meta.location1 = "site";
      msg.meta.location2 = "area";
      msg.meta.location3 = "line";
      msg.meta.location4 = "workcell";
      msg.meta.datacontract = "_analytics";
      msg.meta.path0 = "work_order";  // Logical grouping

      msg.payload = {
        "work_order_id": msg.payload.work_order_id,
        "work_order_start_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_start_time"),
        "work_order_end_time": umh.getLastPayload("enterprise.site.area.line.workcell._historian.workorder.work_order_end_time")
      };

      return msg;
```

