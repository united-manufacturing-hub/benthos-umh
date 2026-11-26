# OPC UA (Output)

The **OPC UA output** plugin writes data into an OPC UA server (e.g., a PLC). There are two ways to make a write safe: the **read-back handshake** (deprecated, still supported — it re-reads a node after writing) and the **programmable handshake**, where you write the machine's own acknowledgment procedure as a small script and the output follows it once per message (see [The Programmable Handshake](#the-programmable-handshake)). The plugin also supports dynamic configuration through interpolated fields.

> **Data Transformations**\
> It is recommended to perform JSON-to-field transformations _before_ this plugin (e.g., via Node-RED JavaScript or [Bloblang](https://www.benthos.dev/docs/guides/bloblang/about)). That way, you feed the final fields directly to this plugin without extra logic here.

## Dynamic Configuration Support

The OPC UA output plugin supports interpolated strings for the following fields:
- `nodeId` - Dynamically determine which OPC UA node to write to based on message content
- `dataType` - Dynamically determine the OPC UA data type based on message content

This enables use cases such as:
- Routing writes to different nodes based on message content
- Dynamically selecting data types based on message metadata
- Building flexible, reusable configurations

***

**Basic Configuration**

> **Deprecation notice**
>
> The `nodeMappings` + read-back `handshake` shape below is the original way to write, and it still works. For anything beyond "write and hope it stuck", use the [programmable handshake](#the-programmable-handshake) instead: the deprecated fields cannot be combined with the new ones, and node mappings cannot be combined with a script.

```yaml
output:
  opcua:
    # endpoint, username, password, securityMode, securityPolicy, serverCertificateFingerprint, clientCertificate
    # see OPC UA Input for more information

    # Static configuration example
    nodeMappings:
      - nodeId: "ns=2;s=MySetpoint"
        valueFrom: "setpoint"   # The JSON field to write
        dataType: "Int32"       # OPC UA data type (required)
      - nodeId: "ns=2;s=MyEnableFlag"
        valueFrom: "enable_flag"
        dataType: "Boolean"     # OPC UA data type (required)

    handshake:
      enabled: true               # enable read-back
      readbackTimeoutMs: 2000     # how long to wait for the new value to appear

      maxWriteAttempts: 3         # how many times to retry if the write fails
      timeBetweenRetriesMs: 1000  # time (ms) between write retries
```

**Supported Data Types**

The OPC UA output plugin supports the following commonly used data types for writing to OPC UA servers:

* `Boolean`: True/false values
* `Byte`: 8-bit unsigned integer (0 to 255)
* `SByte`: 8-bit signed integer (-128 to 127)
* `Int16`: 16-bit signed integer
* `UInt16`: 16-bit unsigned integer
* `Int32`: 32-bit signed integer
* `UInt32`: 32-bit unsigned integer
* `Int64`: 64-bit signed integer
* `UInt64`: 64-bit unsigned integer
* `Float`: 32-bit floating-point number
* `Double`: 64-bit floating-point number
* `String`: UTF-8 encoded string
* `DateTime`: Date and time values

**Note:** When selecting a data type, ensure it matches the expected type on the OPC UA server. Mismatched types may cause write operations to fail or data to be interpreted incorrectly.

**Fields:**

| Field                              | Description                                                                                                                                                                                                                                                                                                                                                                              |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **nodeMappings** (array)           | <p>List of nodes to write to, each with:<br>- <code>nodeId</code>: The OPC UA Node ID, e.g., <code>"ns=2;s=MyVariable"</code><br>- <code>valueFrom</code>: The JSON field name (string) in your message containing the final value.<br>- <code>dataType</code>: The OPC UA data type for the value (e.g., <code>"Boolean"</code>, <code>"Int32"</code>, <code>"Double"</code>, etc.)</p> |
| **handshake**                      | (Optional) The handshake block. It carries either the deprecated read-back fields below or the programmable fields — never both (see [The Programmable Handshake](#the-programmable-handshake)).                                                                                                                                                                                                                                                     |
| **handshake.enabled**              | **Deprecated.** Default `true` (for safe writes). If `true`, the plugin attempts to read the node(s) after writing. If the new value doesn’t match, the write is considered failed.                                                                                                                                                                                                                      |
| **handshake.readbackTimeoutMs**    | **Deprecated.** How long to wait for the server to show the updated value. If it times out, the plugin fails (Benthos will not ACK the message). At least 1 ms and at most an hour; a longer one is refused when the config loads, because it is a unit mistake far more often than an intention.                                                                                                        |
| **handshake.maxWriteAttempts**     | **Deprecated.** Number of write attempts if the server fails (default 1).                                                                                                                                                                                                                                                                                                                                |
| **handshake.timeBetweenRetriesMs** | **Deprecated.** Delay between write attempts (default 1000 ms). At most an hour, for the same reason as `readbackTimeoutMs`.                                                                                                                                                                                                                                                                             |

***

**Usage Example**

**Incoming Message** (e.g., from a Benthos input or pipeline):

```json
{
  "setpoint": 123,
  "enable_flag": true
}
```

**Plugin Configuration**:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://192.168.0.10:4840"
    nodeMappings:
      - nodeId: "ns=2;s=MySetpoint"
        valueFrom: "setpoint"
        dataType: "Int32"
      - nodeId: "ns=2;s=MyEnableFlag"
        valueFrom: "enable_flag"
        dataType: "Boolean"

    handshake:
      enabled: true
      readbackTimeoutMs: 2000
      maxWriteAttempts: 3
      timeBetweenRetriesMs: 1000
```

**Behavior**:

1. The plugin connects to the OPC UA server at `opc.tcp://192.168.0.10:4840`.
2. It **writes** the field `setpoint=123` to node `ns=2;s=MySetpoint` and `enable_flag=true` to `ns=2;s=MyEnableFlag`.
3. Immediately after, it **reads** these nodes back:
   * If the server now reports `MySetpoint=123` and `MyEnableFlag=true`, the write is considered successful.
   * If the read times out after 2 s or shows a different value, the plugin fails the write. Benthos will _not_ ACK the message upstream, so it can be retried or routed to an error output.
4. Assuming success, the message is **acknowledged** and removed from the pipeline.

If, for example, the `ns=2;s=MyEnableFlag` node is read-only or the server rejects the update, the read-back will fail, causing the plugin to retry up to 3 times (`maxWriteAttempts`). If all attempts fail, Benthos escalates the failure.

***

**Handshake & Acknowledgment**

> This section describes the **deprecated** read-back handshake. For the programmable handshake — the machine's own procedure, written as a script — see [below](#the-programmable-handshake).

By default, the plugin **reads back** each node it wrote to confirm the new value appears. This ensures:

1. **Benthos Message ACK**: If the read-back fails or times out, the output plugin fails. Benthos will _not_ acknowledge the message upstream, and you can configure fallback or retry strategies.
2. **Consistent Setpoints**: If the OPC UA server discards or modifies the value, you’ll see an immediate error.

> **Disable** the handshake by setting `handshake.enabled: false` if you prefer no read-back check (faster, but less safe).

**Example**: If you disable the handshake:

```yaml
handshake:
  enabled: false
```

The plugin will write to the OPC UA server but **not** confirm. It will “succeed” as soon as the write request is sent.

***

## The Programmable Handshake

The read-back handshake answers one question: did the *server* take the value? It cannot answer the one that matters at the machine: did the machine *act* on it? A PLC answers that with its own procedure — request flag up, wait for the machine’s confirmation, clear the flag again — and every machine’s procedure is different. The programmable handshake is where you write that procedure down, as a small script, and the output follows it once per message: one message in, several reads, writes and waits against the machine, and one answer out — **delivered**, **try again later**, or **this one cannot work, park it**.

**Basic shape**:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://192.168.0.10:4840"

    handshake:
      maxScriptRuntimeMs: 120000     # the script’s deadline; must clear the waits the script performs
      maxOnSuccessRuntimeMs: 15000
      maxOnErrorRuntimeMs: 15000

      script: |
        # the machine’s procedure, in JavaScript — see below
      onSuccess: |
        # runs after the script returned success
      onError: |
        # runs whenever it did not
```

There are no `nodeMappings` here: the script performs the writes.

### Configuration

| Field | Required | Default when omitted | Description |
|---|---|---|---|
| `handshake.script` | yes | — (the config is refused without one) | The machine’s procedure, in JavaScript. Runs once per message; the script’s own variables are reset for the next one. Must end by returning one of the [four endings](#the-four-endings-and-what-each-one-makes-the-flow-do). A script that does not compile is refused when the pipeline starts, so it never reaches the machine. |
| `handshake.onSuccess` | no | empty | Runs after the script returned success. Usually drops the flag that made the machine act. May be empty. |
| `handshake.onError` | no | empty | Runs after every other ending, the deadline included. Must return the machine to a state it can be asked from. May be empty. |
| `handshake.maxScriptRuntimeMs` | no | `120000` | The script’s deadline. It stops a runaway script, it does not pace the machine: set it comfortably above the waits the script performs. A config whose literal `wait_until` timeouts add up to more than this is refused when the pipeline starts. |
| `handshake.maxOnSuccessRuntimeMs` | no | `15000` | `onSuccess`’s deadline. 15 s clears a flag over a slow link and still bounds a stuck cleanup: a cleanup block is one or two writes, never a wait. |
| `handshake.maxOnErrorRuntimeMs` | no | `15000` | `onError`’s deadline, same reasoning. |

All three deadlines must be positive when you set them: `0` is refused. “No deadline” on a cleanup block would mean an output that can stall forever with nothing in the log, so the config surface does not allow it.

> **Two shapes, one key**
>
> The `handshake` block carries either the deprecated read-back fields (`enabled`, `readbackTimeoutMs`, `maxWriteAttempts`, `timeBetweenRetriesMs`) **or** the programmable fields above — never both. A config that sets one of each is refused when it is loaded, and the error names the two fields that conflict. `nodeMappings` belongs to the deprecated shape too: the script performs the writes, so the two cannot be combined.

### The address object

Every call takes an address — one JavaScript object, written in OPC UA’s own words:

```js
{ nodeId: "ns=2;s=Machine.Setpoint", dataType: "Int32" }
```

- `nodeId` (required): the node, exactly as you would type it into a client (`ns=2;s=MyVariable`, `ns=3;i=1000`, `i=85`, …).
- `dataType` (required for `write`, optional everywhere else): one of the supported types listed under **Supported Data Types** above — `Boolean`, `SByte`, `Byte`, `Int16`, `UInt16`, `Int32`, `UInt32`, `Int64`, `UInt64`, `Float`, `Double`, `String`, `DateTime`.
- `objectId` (required for `call_method`): the node the method lives on; `nodeId` is then the method itself.

Unknown keys in the object are left alone, so a future option does not break a script already written.

### What the script can call

The script sees the incoming message as `msg` — the message’s JSON, so a message `{"payload": {"value": 42}}` is `msg.payload.value` in the script. It talks to the machine through four functions:

| Function | Returns | Description |
|---|---|---|
| `opcua.read(address)` | the node’s value | `true`/`false` for `Boolean`, a number for the numeric types, a string for `String`, an RFC 3339 string for `DateTime`, an array for an array node. A *Bad* status ends the run by itself; an *Uncertain* status still returns the value — that is how KEPServerEX answers the first read of a healthy tag. |
| `opcua.write(address, value)` | nothing | Sends one value, converted to the address’s `dataType`. Only whole numbers convert to the integer types (`42.0` is fine, `42.5` is refused); `Boolean` takes only `true`/`false`; `String` only a string; `DateTime` a string like `"2024-03-15T12:00:00Z"`. A value that does not convert, or that lies outside the declared type’s range, ends the run as persistent and nothing is sent — it is refused, never rounded or clamped. Large integers keep every digit when the message carries them as JSON numbers (the digits are parsed exactly, over the whole `Int64` and `UInt64` range); values your script *computes* with JavaScript arithmetic are exact up to 2^53, which is the language’s own limit. |
| `opcua.wait_until(address, predicate, timeoutMs, options?)` | the value that satisfied the predicate | Re-reads the node until `predicate(value)` is true, or `timeoutMs` passes. It polls every 100 ms; pass a smaller `{ pollMs: 20 }` as the fourth argument to answer a fast machine sooner, or a larger one to go easier on a slow server. A wait that runs out ends the run as transient (try again later) — pass `{ onTimeout: "persistent" }` when you know the flag will never come. **Inside `onSuccess` or `onError` it is always persistent**, whatever `onTimeout` says, because a wait that runs out in a cleanup block means the machine was never handed back. A wait never runs past the script’s deadline. |
| `opcua.call_method(address, [inputs])` | the method’s output arguments | Calls the OPC UA method `address.nodeId` on `address.objectId`, passing the inputs in order, and returns the outputs as an array, in the order the machine declares them. |

`read`, `write` and `call_method` are one round trip each. A failed call ends the run by itself; you do not need to handle it: a machine that is briefly unreachable ends the run as transient, a wrong address, a wrong type or a value the server refuses ends it as persistent.

**If you want to handle a failure yourself, catch it.** A failed call throws, so an ordinary
`try`/`catch` around it keeps the script running:

```javascript
let serial = null;
try {
  serial = opcua.read({ nodeId: "ns=2;s=Serial" });   // not fitted on every machine
} catch (e) {
  // this line is optional on this press; carry on without it
}
opcua.write(SETPOINT, msg.payload.value);
return opcua.success({ serial: serial });
```

Catching tells the output the failure was expected, and the ending your script returns is
then the answer the flow gets. The example above delivers the message even though the read
failed.

That cuts both ways, so catch the calls you know can fail rather than wrapping the whole
script. A `catch` around a write that the machine refused hides a refusal that should have
parked the message, and the flow is told the machine got data it never got.

### The four endings, and what each one makes the flow do

The script ends by **returning** one of four endings. The ending is the answer the output gives to the flow around it, and the flow does the work:

| Ending | What the flow does |
|---|---|
| `return opcua.success(obj)` | **Delivered.** The message is acknowledged and leaves the pipeline. `obj` is the result — the success flow publishes it. `obj` is optional. |
| `return opcua.error_not_ready()` | **Try again later.** The machine is not in a state to be asked — still busy, not in remote. An ordinary answer from a working machine. |
| `return opcua.error_transient(detail)` | **Try again later.** Something that may fix itself went wrong. |
| `return opcua.error_persistent(detail)` | **Park it.** Repeating cannot fix this — wrong data, a machine that refuses the value. The message belongs where a person sees it. |

`detail` is optional; it is a string or an object, for whoever reads the error.

Three more endings exist that the script does not choose:

| What happened | What the flow does |
|---|---|
| a call failed because the machine was unreachable | **Try again later.** |
| a call failed because the script or its data was wrong | **Park it.** |
| the deadline (`maxScriptRuntimeMs`) stopped the script | **Park it** — the deadline is configuration, so repeating cannot fix it. `onError` still runs. |

> **This output never retries by itself.** It hands the message back with the answer above and has no retry setting. Without a flow that retries the “try again later” ones and parks the “park it” ones — for example a `retry` output around it with a fallback — a “try again later” is a write request nobody ever delivers. Plan the flow around the output when you plan the script.

### `onSuccess` and `onError`

Exactly one of the two blocks runs after every message:

- `onSuccess` runs when the script returned success.
- `onError` runs after every other ending — including one stopped by the deadline, which no line *inside* the script can cover, and which is why these are config blocks rather than a `finally`.

The blocks see nothing but the machine: no `msg`, and no variables from the script. They use the same four `opcua.*` functions, and they cannot end a run — their only job is to return the machine to its idle state.

A block that does not finish changes the outcome. It has not finished if its deadline ran out, if a write or a `call_method` it asked for came back with an error, or if a `wait_until` inside it ran out: in each case the machine was not returned to its idle state, and nothing afterwards can say what state it is in.

- after a **failure**, the run becomes persistent no matter what the failure was, because the machine’s state is now unknown;
- after a **delivery**, the delivery stands — the machine has the data — and the cleanup failure is logged beside it.

A **read** is the exception. A read cannot change the machine, so a block may probe a node, catch the refusal and carry on; that is how a cleanup says “I expected this one to fail”. A read inside a `wait_until` is not exempt, because the wait was abandoned and the work the block meant to do after it never ran.

So write `onError` for the worst moment, not the happy path: it must be safe to run when the script wrote nothing, and when the machine already acted.

### Worked example

A machine loads a parameter set when its `Apply` flag goes up, and answers on its own `Loaded` flag. The message carries the parameters; each parameter node holds its own type.

**Incoming message**:

```json
{
  "payload": {
    "parameters": [
      { "name": "Torque", "dataType": "Double", "value": 12.5 },
      { "name": "Count",  "dataType": "Int32",  "value": 4 }
    ]
  }
}
```

**Configuration**:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://192.168.0.10:4840"
    handshake:
      maxScriptRuntimeMs: 120000
      script: |
        const APPLY  = { nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" };
        const LOADED = { nodeId: "ns=3;s=Params.Loaded" };

        // Reads only, so a run that stops here is safe to try again.
        // Loaded must be down, or a flag left up by the last parameter set
        // would end the wait below at once, for a transfer that never happened.
        if (opcua.read(APPLY) !== false ||
            opcua.read(LOADED) !== false) { return opcua.error_not_ready(); }

        // Each parameter brings its own type, so the address is built per parameter.
        const p = msg.payload.parameters;
        for (let i = 0; i < p.length; i++) {
          opcua.write({ nodeId: "ns=3;s=Params." + p[i].name,
                        dataType: p[i].dataType }, p[i].value);
        }

        // The machine acts only on this write, so it goes last.
        opcua.write(APPLY, true);

        // Production is released only on the machine’s own confirmation.
        opcua.wait_until(LOADED, v => v === true, 10000, { pollMs: 20 });

        return opcua.success({ parameters: p.length, released: true });

      onSuccess: |
        opcua.write({ nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" }, false);

      onError: |
        opcua.write({ nodeId: "ns=3;s=Params.Apply", dataType: "Boolean" }, false);
```

**Behavior**:

1. The script reads `Apply` and `Loaded`. If either is up — the machine is still working on the last set — it ends `error_not_ready`, and the message is handed back for the flow to try again later.
2. It writes each parameter to its own node, in the type the address declares.
3. It raises `Apply`. The machine acts only on this write, which is why it goes last.
4. It waits for `Loaded`, polling every 20 ms, and returns `success` when the machine confirms.
5. `onSuccess` drops `Apply` again, leaving the machine ready for the next message. The message is acknowledged. The object the script returned is written to the output's debug log and goes nowhere else — a Benthos output is the end of the pipeline, so there is no downstream to route it to.
6. If anything along the way fails — the machine unreachable, the wait runs out, a value the server refuses — `onError` drops `Apply`, and the message is handed back carrying the class of the failure, so the flow can retry it or park it.

### Migrating from the read-back fields

The deprecated fields still work, and a config that only sets them behaves exactly as before. Nothing breaks on upgrade. When you are ready:

- **A plain write with no verification** (`handshake.enabled: false`): keep your `nodeMappings` as they are — nothing to migrate.
- **Read-back verification** (`enabled: true`): replace the `nodeMappings` with a script that writes, then reads back and compares —

  ```yaml
  handshake:
    script: |
      const SETPOINT = { nodeId: "ns=2;s=MySetpoint", dataType: "Int32" };
      opcua.write(SETPOINT, msg.setpoint);
      if (opcua.read(SETPOINT) !== msg.setpoint) {
        return opcua.error_persistent("setpoint did not stick");
      }
      return opcua.success({ setpoint: msg.setpoint });
  ```

- **Write retries** (`maxWriteAttempts`, `timeBetweenRetriesMs`): the retries move out of the output, into the flow around it (see the note above). Retrying inside the output could re-run half a handshake against a machine that already acted on the first half.

***

## Delivery guarantees

- **Delivery**: a message is delivered when the script ended `success` — the machine received the write request and acted on it, by the procedure you wrote. A delivered message is acknowledged. The object the script returned reaches the debug log and nothing else: an output is the end of the pipeline, so do not plan to route on it. After a failure the script path never retries by itself: it hands the message back classed transient or persistent, and the flow around it decides. The deprecated read-back path does retry by itself, up to `maxWriteAttempts` (see [the endings](#the-four-endings-and-what-each-one-makes-the-flow-do)).
- **State**: the output keeps no state about a message. A message that arrives twice runs the script again from the top, and nothing recognises the repeat — the readiness check at the top of your script is what makes a repeat safe. What the output does keep — the connection and its settings — lives as long as the output does.
- **Time**: the three deadlines bound the script, `onSuccess` and `onError`. `wait_until` waits at most its own timeout and never past the script’s deadline. No deadline is ever taken from the message.
- **Errors**: transient (machine unreachable, not ready, a wait that ran out) means safe to try again; persistent (wrong data, a refused write, the deadline, a cleanup that did not finish) means park it. The deprecated read-back path fails without a class — retrying it is the flow’s decision too. A lost connection is reported as “not connected”, and what happens next is `autoReconnect`, which is **off** unless you turn it on: left off, the output stays down and is visible as down; turned on, the client rebuilds the session underneath and the next message is written with nothing logged.
- **Limits**: one message is in flight per output; two handshakes on one machine need two outputs. Every read and write is one round trip and there is no batching. A `call_method` is three: the binding browses the method's `InputArguments`, reads that property, then calls — so a handshake that calls a method per message costs three times what a read or a write does. Nothing holds two of the script’s reads to one scan cycle, so a guard that reads two flags sees them from two scans, and the PLC can change the machine between them. Writing an array is not supported; reading an array node returns an array.

***

## Dynamic Configuration Example

The OPC UA output plugin supports dynamic node IDs using interpolated fields:

```yaml
output:
  opcua:
    endpoint: "opc.tcp://localhost:4840"

    # Dynamic node mapping based on message content
    nodeMappings:
      - nodeId: "${! json(\"target_node\") }"     # Node ID from message field
        valueFrom: "value"
        dataType: "${! json(\"dataType\") }"      # Data type from message field
```

**Example with message routing:**

```yaml
pipeline:
  processors:
    - mapping: |
        # Route to different nodes based on tag type
        root.target_node = match this.tag_type {
          "color" => "ns=2;s=[default]/PaintRoom1/Spraytan1/Color",
          "speed" => "ns=2;s=[default]/PaintRoom1/Spraytan1/Speed",
          "temp"  => "ns=2;s=[default]/PaintRoom1/Spraytan1/Temperature"
        }
        # Set appropriate data type based on tag type
        root.target_datatype = match this.tag_type {
          "color" => "String",
          "speed" => "Double", 
          "temp"  => "Float"
        }
        root.value = this.tag_value

output:
  opcua:
    endpoint: "opc.tcp://plc.factory.local:4840"
    nodeMappings:
      - nodeId: "${! json(\"target_node\") }"
        valueFrom: "value"
        dataType: "${! json(\"target_datatype\") }"
```

This enables building reusable configurations that adapt to different environments and message types without hardcoding node paths.

***

**Implementation Details & Future Outlook**

For many industrial use cases, you might need more than just writing a value and reading it back:

1. **De-duplication**: If you re-send the same “command” multiple times, do you want the PLC to ignore duplicates?
   * _Script path_: Write a unique command ID to a node, read it back as part of the handshake, and refuse a message whose ID was already taken — the programmable handshake is where that check lives.
   * _Fixed path_: Implement a unique command ID (UUID) in your message and let the PLC store/ignore duplicates. Or handle it in your Benthos pipeline (e.g., a “dedupe” processor).
2. **Time-Window Checks**: Only accept a command if it arrives before a certain expiration.
   * _Script path_: Read a timestamp node in the script and end `error_persistent` when the command is too old — or drop old messages before they reach the output. Use `error_persistent` and not `error_not_ready` here: a command that is too old cannot become less old, so “try again later” sends the message round the retry loop until the flow gives up, re-running the script against the machine every time.
   * _Fixed path_: Use a preceding nodered\_js processor or Bloblang to drop the message if `timestamp_now - msg.timestamp > threshold`.
3. **Separate Acknowledgment Node**: Some PLCs use a separate ack node (e.g., `CommandAck`) that signals the command was _processed_.
   * _Shipped_: The [programmable handshake](#the-programmable-handshake) is exactly this — request flags, ack nodes, waits on the machine’s own confirmation, all as reads, writes and `wait_until` in the script.

With Benthos, the “at least once” acknowledgment ensures that if writing fails, the message can be retried or routed. The fixed read-back handshake remains a safe start for a single setpoint; once the machine has a procedure of its own, write it down as a script and let the output follow it.
