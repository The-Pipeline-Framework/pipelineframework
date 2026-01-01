# Pipeline Aspect Application Semantics

## Overview
Pipeline aspects are cross-cutting semantic concerns (e.g. persistence, metrics, tracing) that apply around pipeline steps without altering the pipeline's functional shape.

## Core Principles
- Aspects do not change:
  - Pipeline inputs/outputs
  - Step cardinality
  - Streaming shape
- Aspects are logically expanded into identity side-effect steps
- Expansion is semantic only (not user-visible)

## Application Scopes

### GLOBAL Scope
- Aspects with GLOBAL scope apply to all steps present in the pipeline definition at generation time
- Future steps added later require regeneration
- They are applied consistently across every step in the pipeline
- This is useful for concerns like global metrics collection, tracing, or persistence

### STEPS Scope
- STEPS scope is reserved for future extensions
- In the current version, STEPS scope MUST be empty or treated as GLOBAL with a warning
- This provides fine-grained control over which steps have specific aspects applied in future versions
- This is useful for concerns that should only apply to specific steps (e.g., persistence only for certain business operations)

## Logical Expansion
Aspects are conceptually expanded into identity side-effect steps in the pipeline:
- BEFORE_STEP aspects become side-effect steps that execute before the main step
- AFTER_STEP aspects become side-effect steps that execute after the main step
- These expansions are purely semantic and not visible to the user
- The pipeline's functional contract remains unchanged

## Aspect Invariants
- Aspects do not change pipeline types or topology
- Aspects may have side effects
- Aspects may observe or persist data
- Aspects are not allowed to alter pipeline control flow
- Any aspect that does require data transformation must be modeled as a Step, not an Aspect