---
search: false
---

# Pipeline Framework Compiler Architecture Evolution

## Overview

This document describes the evolution of the Pipeline Framework annotation processor into a multi-phase compiler pipeline. The refactoring transforms a monolithic processor into a modular, compiler-inspired architecture with distinct phases, each responsible for a specific aspect of code generation.

## Motivation

The original PipelineStepProcessor had grown to over 400 lines and violated the single-responsibility principle by mixing:
- Discovery logic
- Semantic analysis
- Target resolution
- Binding construction
- Artifact generation
- Infrastructure concerns

This led to:
- Difficult maintenance
- Poor testability
- Tight coupling between concerns
- Complex debugging

## New Architecture: Multi-Phase Compiler Pipeline

The refactored architecture follows a compiler-style pipeline with distinct phases:

```text
Annotations -> Discovery -> Semantic Analysis -> Target Resolution -> Binding Construction -> Generation -> Infrastructure
```

Each phase has a clearly defined responsibility and does not leak concerns into adjacent layers.

## Phase Architecture

### 1. PipelineDiscoveryPhase

**Responsibilities:**
- Discovers annotated elements (@PipelineStep, @PipelinePlugin, @PipelineOrchestrator)
- Loads pipeline configuration from YAML files
- Determines transport mode (gRPC/REST)
- Discovers orchestrator models
- Sets plugin host flag

**Key methods:**
- `loadPipelineAspects()`
- `loadPipelineTemplateConfig()`
- `loadPipelineTransport()`
- `resolveOrchestratorAnnotation()`

**Constraints:**
- No validation
- No code generation
- No binding construction

### 2. PipelineSemanticAnalysisPhase

**Responsibilities:**
- Analyzes semantic models for policy decisions
- Identifies cache aspects for expansion
- Determines orchestrator generation requirements
- Calculates streaming shapes
- Sets flags in compilation context

**Key methods:**
- `streamingShape()`
- `isStreamingInputCardinality()`
- `applyCardinalityToStreaming()`
- `isCacheAspect()`
- `shouldGenerateOrchestrator()`
- `shouldGenerateOrchestratorCli()`

**Constraints:**
- Never builds bindings
- Never calls renderers
- Never modifies semantic models

### 3. PipelineTargetResolutionPhase

**Responsibilities:**
- Determines which GenerationTargets apply
- Decides client/server roles
- Updates models with appropriate targets and roles
- Resolves transport-specific targets

**Key methods:**
- `resolveTransportTargets()`
- `applyTransportTargets()`
- `resolveClientRole()`

**Constraints:**
- No binding construction
- No JavaPoet usage
- No file generation

### 4. PipelineBindingConstructionPhase

**Responsibilities:**
- Constructs renderer-specific bindings from semantic models
- Builds gRPC, REST, and orchestrator bindings
- Handles plugin host expansion logic
- Creates immutable, renderer-scoped bindings

**Key methods:**
- `rebuildGrpcBinding()`
- `rebuildRestBinding()`
- `buildOrchestratorBinding()`
- `buildPluginHostSteps()`
- `buildPluginHostStep()`
- `resolveCacheKeyGenerator()`

**Constraints:**
- No code generation
- No file I/O
- No semantic model mutation

### 5. PipelineGenerationPhase

**Responsibilities:**
- Iterates over GenerationTargets
- Delegates to PipelineRenderer implementations
- Passes semantic models, bindings, and GenerationContext
- Handles artifact generation

**Key methods:**
- `generateArtifacts()`
- `generateSideEffectBean()`
- `generateOrchestratorServer()`

**Constraints:**
- No JavaPoet logic
- No complex decisions
- No binding construction

### 6. PipelineInfrastructurePhase

**Responsibilities:**
- Resolves filesystem paths
- Creates role-specific output directories
- Populates compilation context with infrastructure info

**Key methods:**
- `resolveModuleDir()`
- `resolveGeneratedSourcesRoot()`
- `resolveRoleOutputDir()`

**Constraints:**
- No semantic logic
- No code generation

## Collaborator Pattern

Each phase delegates to focused collaborators that handle specific concerns:

### StepBindingBuilder
- `rebuildGrpcBinding()`
- `rebuildRestBinding()`

### PluginBindingBuilder
- `buildPluginHostSteps()`
- `buildPluginHostStep()`
- `extractPluginAspectNames()`

### OrchestratorBindingBuilder
- `buildOrchestratorBinding()`
- `resolveOrchestratorAnnotation()`

### StreamingShapeResolver
- `streamingShape()`
- `applyCardinalityToStreaming()`
- `isStreamingInputCardinality()`

### CacheKeyResolver
- `resolveCacheKeyGenerator()`

### NamingPolicy
- `toPascalCase()`
- `stripProcessPrefix()`
- `formatForClassName()`
- `emptyToNull()`

## Compilation Context

The `PipelineCompilationContext` serves as the central data structure that flows through all phases:

**Fields:**
- `ProcessingEnvironment processingEnv`
- `RoundEnvironment roundEnv`
- `List<PipelineStepModel> stepModels`
- `List<PipelineAspectModel> aspectModels`
- `List<PipelineAspectModel> aspectsForExpansion`
- `List<PipelineOrchestratorModel> orchestratorModels`
- `Object pipelineTemplateConfig`
- `Set<GenerationTarget> resolvedTargets`
- `Map<String, Object> rendererBindings`
- `Path generatedSourcesRoot`
- `Path moduleDir`
- `boolean pluginHost`
- `boolean orchestratorGenerated`
- `boolean transportModeGrpc`

## Compiler Interface

All phases implement the `PipelineCompilationPhase` interface:

```java
public interface PipelineCompilationPhase {
    String name();
    void execute(PipelineCompilationContext ctx) throws Exception;
}
```

## Pipeline Compiler

The `PipelineCompiler` orchestrates the phases:

```java
public class PipelineCompiler extends AbstractProcessingTool {
    private final List<PipelineCompilationPhase> phases;
    
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        PipelineCompilationContext context = new PipelineCompilationContext(processingEnv, roundEnv);
        
        for (PipelineCompilationPhase phase : phases) {
            try {
                phase.execute(context);
            } catch (Exception e) {
                processingEnv.getMessager().printMessage(ERROR, 
                    "Pipeline compilation failed in phase '" + phase.name() + "': " + e.getMessage());
                return true;
            }
        }
        return true;
    }
}
```

## Benefits of the New Architecture

### 1. Single Responsibility
Each phase has a single, well-defined responsibility, making the code easier to understand and maintain.

### 2. Testability
Individual phases can be tested in isolation with focused unit tests.

### 3. Extensibility
New phases can be added without modifying existing ones.

### 4. Maintainability
Changes to one concern don't affect others due to clear separation of responsibilities.

### 5. Debugging
Issues can be traced to specific phases, simplifying debugging.

## Design Principles

### Phase Isolation
Phases must not leak concerns into adjacent phases. Each phase operates on the compilation context and its specific responsibilities.

### Immutable Models
Semantic models remain immutable after creation. Phases may create new models but must not modify existing ones.

### No Cross-Phase Dependencies
Phases should not depend on the internal workings of other phases beyond the shared compilation context.

### Clear Data Flow
Data flows linearly through the phases via the compilation context, with each phase adding or transforming information as needed.

## Migration Path

The refactoring preserves all existing behavior while improving the internal architecture. The original `PipelineStepProcessor` functionality is maintained through the new phase-based approach.

## Future Evolution

This architecture enables:
- Easy addition of new phases
- Pluggable phase execution
- Conditional phase execution based on context
- Enhanced testing capabilities
- Better error reporting per phase

## Conclusion

The multi-phase compiler pipeline architecture successfully transforms the monolithic annotation processor into a modular, maintainable system. Each phase has clear responsibilities, the code is highly testable, and the architecture supports future growth while maintaining backward compatibility.
