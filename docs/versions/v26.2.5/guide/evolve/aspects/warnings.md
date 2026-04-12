---
search: false
---

# Semantic Warnings for Pipeline Aspects

The following are semantic warnings (not errors) that the compiler/processor may emit:

## Potential Issues

1. **Multiple GLOBAL aspects with identical order**
   - Warning: When multiple GLOBAL aspects have the same order value, their relative execution order is implementation-dependent
   - Impact: May cause unexpected interaction between aspects

2. **Aspect applied AFTER a REDUCTION step**
   - Warning: Aspects applied AFTER reduction steps may have batching implications
   - Impact: Aspects might execute after batching has occurred, affecting metrics or other concerns

3. **Aspect configuration present but enabled=false**
   - Warning: When an aspect has configuration defined but is disabled (enabled=false)
   - Impact: Configuration is present but unused, potentially indicating an oversight

4. **STEPS scope with content**
   - Warning: STEPS scope is reserved for future extensions; in the current version, STEPS scope MUST be empty or treated as GLOBAL with a warning
   - Impact: Future behavior may change when STEPS scope is fully implemented

## Aspect Invariants

The following invariants define the fundamental behavior of pipeline aspects:

- Aspects do not change pipeline types or topology
- Aspects may have side effects
- Aspects may observe or persist data
- Aspects are not allowed to alter pipeline control flow
- Any aspect that does require data transformation must be modeled as a Step, not an Aspect

These warnings are not validation errors and will not prevent compilation, but they may indicate potential configuration issues that users should review.