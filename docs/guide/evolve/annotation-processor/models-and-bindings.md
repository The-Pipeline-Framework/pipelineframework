# Models and Bindings

## Core Semantic Model

`PipelineStepModel` is the primary IR unit.

It captures semantic step intent (name, package, type mappings, streaming shape, execution mode, deployment role, generation targets) without transport-specific adapter implementation details.

Related IR types:

- `StreamingShape`
- `GenerationTarget`
- `ExecutionMode`
- `DeploymentRole`
- `TypeMapping`

## Compilation Context Contract

`PipelineCompilationContext` is the phase handoff object and contains:

- discovered models: `stepModels`, `aspectModels`, `orchestratorModels`
- mapping state: `runtimeMapping`, `runtimeMappingResolution`
- mode state: transport + platform
- binding state: `rendererBindings`
- infra state: output roots, module dir/name
- generation state: descriptor set, plugin/orchestrator flags

## Binding Types

Bindings are transport/rendering realization objects:

- `GrpcBinding`
- `RestBinding`
- `LocalBinding`
- `OrchestratorBinding`

Bindings are produced by `StepBindingBuilder` (invoked from `PipelineBindingConstructionPhase`) and stored in `rendererBindings` using key constants:

- `StepBindingBuilder.GRPC_SUFFIX` → `<service>_grpc`
- `StepBindingBuilder.REST_SUFFIX` → `<service>_rest`
- `StepBindingBuilder.LOCAL_SUFFIX` → `<service>_local`
- `StepBindingBuilder.ORCHESTRATOR_KEY` → `orchestrator`

## Binding Construction Flow

```mermaid
flowchart TD
    A["PipelineStepModel + Targets"] --> B["PipelineBindingConstructionPhase"]
    B --> C["GrpcRequirementEvaluator"]
    C -->|needs gRPC| D["DescriptorFileLocator"]
    C -->|no gRPC| E["Skip descriptor load"]
    D --> F["StepBindingBuilder.constructBindings"]
    E --> F
    F --> G["GrpcBindingResolver"]
    F --> H["RestBindingResolver"]
    F --> I["LocalBinding creation"]
    B --> J["OrchestratorBindingBuilder"]
    G --> K["rendererBindings map"]
    H --> K
    I --> K
    J --> K
```

## Mapping + Role Interplay

- Runtime mapping can filter which step models remain for a module.
- Target resolution then derives target sets from `(deploymentRole, transportMode)`.
- Binding construction only builds bindings required by those targets.

This keeps expensive binding work scoped to relevant artifacts.
