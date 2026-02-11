# Current Architecture

This page summarizes the current structure of the annotation processor in `framework/deployment`.

## Active Compiler Path

The canonical processor path is:

1. `PipelineDiscoveryPhase`
2. `ModelExtractionPhase`
3. `PipelineRuntimeMappingPhase`
4. `PipelineSemanticAnalysisPhase`
5. `PipelineTargetResolutionPhase`
6. `PipelineBindingConstructionPhase`
7. `PipelineGenerationPhase`
8. `PipelineInfrastructurePhase`

This chain is wired in `PipelineStepProcessor.init(...)` and executed by `PipelineCompiler`.

## Active vs Retained Components

Active in canonical path:

- `PipelineStepProcessor`
- `PipelineCompiler`
- `PipelineCompilationContext`
- the eight phases listed above

Retained in codebase but not in the default phase chain:

- `ConfigurationLoadingPhase`
- `TargetResolutionPhase`
- `BindingResolutionPhase`

These retained classes are not the canonical execution path unless explicitly wired.

## Generation Dispatch Model

The generation subsystem contains:

- per-target generator abstractions (`TargetGenerator`, `GenerationRequest`, concrete `*TargetGenerator` classes)
- domain services (`SideEffectBeanService`, `ProtobufParserService`, `OrchestratorGenerationService`)
- the generation phase orchestrator (`PipelineGenerationPhase`)

```mermaid
flowchart LR
    A["PipelineGenerationPhase"] --> B["Bindings + Targets"]
    B --> C["Renderers"]
    B --> D["SideEffectBeanService"]
    B --> E["OrchestratorGenerationService"]
    B --> F["ProtobufParserService"]
    C --> G["Generated Sources"]
    D --> G
    E --> G
    F --> G
```

## Package Map

- `processor/`: processor facade and compiler orchestration
- `processor/ir/`: semantic and binding model types
- `processor/phase/`: phase implementations and generation services/policies
- `processor/extractor/`: annotation -> IR extraction
- `processor/config/`: YAML and option loaders
- `processor/renderer/`: JavaPoet renderers
- `processor/util/`: path, metadata, naming, binding helpers
- `processor/mapping/`: runtime mapping resolver/model
- `processor/validator/`: validation helpers

## Guardrails

- Keep semantic IR transport-agnostic.
- Keep transport-specific realization in bindings/renderers/services.
- Keep policy decisions centralized (semantic analysis + generation policy).
- Keep phase responsibilities isolated and explicit.
