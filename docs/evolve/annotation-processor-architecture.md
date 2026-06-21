# Annotation Processor Architecture

This page links to the split guide:

- [Annotation Processor Guide: Overview](/evolve/annotation-processor/)
- [Phases and Flow](/evolve/annotation-processor/phases-and-flow)
- [Models and Bindings](/evolve/annotation-processor/models-and-bindings)
- [Generation and Rendering](/evolve/annotation-processor/generation-and-rendering)
- [Current Architecture](/evolve/annotation-processor/current-architecture)

```mermaid
flowchart TD
    A["PipelineStepProcessor"] --> B["PipelineCompiler"]
    B --> P1["Discovery"]
    P1 --> P2["Model Extraction"]
    P2 --> P3["Runtime Mapping"]
    P3 --> P4["Semantic Analysis"]
    P4 --> P5["Target Resolution"]
    P5 --> P6["Binding Construction"]
    P6 --> P7["Generation"]
    P7 --> P8["Infrastructure"]
```

Related:

- [Compiler Pipeline Architecture](/evolve/compiler-pipeline-architecture)
