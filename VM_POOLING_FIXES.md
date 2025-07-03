# VM Pooling Optimization - Final Implementation

This document outlines the VM pooling optimization applied to address the VM state leakage issue in the Benthos UMH project's JavaScript processors.

## Issues Identified and Fixed

### VM State Leakage ✅ RESOLVED

**Problem**: VM cleanup only called `vm.ClearInterrupt()` but didn't clear the `msg` object, leading to:
- Persistent `msg` variables and properties between messages
- Potential cross-message data contamination
- Security and data integrity concerns

**Solution**: Implemented targeted VM cleanup + strict mode enforcement:
```go
func (u *NodeREDJSProcessor) clearVMState(vm *goja.Runtime) {
    vm.ClearInterrupt()
    vm.GlobalObject().Set("msg", nil)  // Clear msg object
}
```

```go
// User code wrapped with strict mode
wrappedCode := fmt.Sprintf(`
    (function(){
        'use strict';
        %s
    })()
`, code)
```

## Key Enhancements

### 1. Targeted VM Cleanup
- **Focus on the real issue**: Only the `msg` object needed cleanup
- **Minimal overhead**: Just setting one variable to nil
- **Prevents state leakage**: No cross-message contamination

### 2. Strict Mode Enforcement ✅ NEW
- **Prevents accidental globals**: `myVar = 5` throws error instead of creating global
- **Improves security**: Blocks dangerous JavaScript operations
- **Better error handling**: Catches more mistakes at execution time
- **Complements cleanup**: Prevents other forms of state leakage

### 3. Thread Safety - Leveraged Built-in Solutions
- **sync.Pool is inherently thread-safe** - no additional synchronization needed
- Each `Get()` call returns a different VM instance to different goroutines
- `defer putVM(vm)` pattern already handles panics correctly

## Implementation Details

### VM Pool Configuration
```go
// Simple pool configuration - no custom New function needed
vmpool: sync.Pool{}, // Get() returns nil when empty, we handle it
```

### VM Lifecycle with Strict Mode
```go
// Get VM from pool
vm := u.getVM()
defer u.putVM(vm)

// User code executed in strict mode automatically
result := u.processSingleMessage(vm, msg)

// VM automatically cleaned before being returned to pool
```

### Cleanup Process
1. **Clear interrupt flags** - prevents stuck VM states
2. **Clear msg object** - prevents cross-message contamination
3. **Return to pool** - ready for reuse (strict mode enforced for next use)

## Testing

Added comprehensive tests to verify:
- `msg` object properties don't leak between messages
- VM reuse works correctly
- Multiple message processing is isolated
- **Strict mode prevents accidental global variables** ✅ NEW

## Strict Mode Benefits

### What It Prevents
```javascript
// ❌ These now throw errors instead of creating globals
accidentalGlobal = "oops";
myVar = 5;  // forgot var/let/const
```

### Safe Patterns Still Work
```javascript
// ✅ These continue to work fine
var localVar = 5;
let scopedVar = 10;
const constVar = 15;
msg.payload = "processed";
return msg;
```

## Benefits

✅ **Eliminates state leakage** - No more cross-message contamination
✅ **Prevents accidental globals** - Strict mode catches common mistakes
✅ **Improves security** - Blocks dangerous JavaScript operations
✅ **Better error handling** - Fails fast instead of silently creating problems
✅ **Maintains performance** - VM pooling still provides performance benefits  
✅ **Simplified code** - No unnecessary complexity
✅ **Reliable operation** - Leverages Go's proven sync.Pool implementation

## Files Modified

- `nodered_js_plugin/nodered_js_plugin.go` - Added strict mode + simplified VM cleanup
- `nodered_js_plugin/nodered_js_plugin_test.go` - Added state cleanup & strict mode tests
- `tag_processor_plugin/tag_processor_plugin.go` - Added strict mode to all code execution paths

## Performance Impact

- **Minimal overhead** - Only setting one variable to nil + strict mode parsing
- **VM pooling benefits maintained** - Still reuses VMs for performance
- **No complex JavaScript execution** - Just a simple global object property set
- **Improved reliability** - Prevents silent failures and state corruption

## Migration Impact

- **Backward compatible** for well-written code
- **May break code** that relied on accidental global variable creation
- **Recommended**: Test existing JavaScript code to ensure strict mode compatibility
- **Users benefit**: More robust and predictable JavaScript execution 