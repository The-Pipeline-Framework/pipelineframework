# Annotation Processing and Transport

The annotation processor (AP) runs per module, but runtime mapping is global. The processor must resolve the full mapping, then filter to the current module.

## Resolution flow (per module)

1. Load `pipeline.runtime.yaml` if present.
2. Build the global step and synthetic index from annotations/aspects.
3. Resolve placement for each step/synthetic:
   - explicit mapping
   - otherwise defaults (if `validation: auto`)
4. Filter the resolved set to the current module.
5. Generate server and client artifacts for the module.

## Transport and client wiring

- Each module belongs to a runtime.
- Transport is configured globally in `pipeline.yaml` and applies to the whole pipeline.
- Cross-module calls use the pipeline transport (gRPC or REST).
- Calls within a module use direct in-process wiring (current behavior).

## Monolith layout

- Logical placement resolves all services/plugins to one module.
- Client wiring is generated as in-process for that single-module target.
- This becomes a real monolith runtime only if the Maven/build topology is also monolithic.
- Transport settings remain relevant only for external exposure (if any).

## Pipeline-runtime layout

- Steps within the same pipeline must resolve to a single runtime.
- The orchestrator remains a separate runtime (current default scaffolding behavior).
- As with monolith, this is logical placement first; physical deploy units depend on build topology.

## Backward compatibility

If the mapping file is not present or `validation: auto` with no explicit placements, the processor must behave exactly as it does today.
