---
search: false
---

# Orchestrator Runtime

The Pipeline Framework generates a single orchestrator runtime that coordinates the pipeline flow. It provisions inputs, invokes step clients, and handles the final outputs.

<Callout type="tip" title="Canvas First">
Configure the orchestrator flow in the Canvas at <a href="https://app.pipelineframework.org" target="_blank">https://app.pipelineframework.org</a>, then customize the generated runtime as needed.
</Callout>

## What It Does

1. Initiates pipeline execution
2. Provides input data
3. Coordinates step-to-step flow
4. Handles final outputs

## Generated Structure

```text
orchestrator-svc/
├── pom.xml
├── src/main/java/
│   └── com/example/app/orchestrator/
│       └── OrchestratorApplication.java
└── src/main/resources/
    └── application.properties
```

The generated `OrchestratorApplication` is annotated with `@PipelineOrchestrator` so the annotation processor
can generate the server endpoint (REST or gRPC, based on `pipeline-config.yaml`) at compile time.

## Input Options

The generated runtime supports several ways to specify input:

1. **CLI argument**: `-i` / `--input`
2. **Environment variable**: `PIPELINE_INPUT`
3. **Quarkus config**: `quarkus.pipeline.input`
4. **System property**: `-Dquarkus.pipeline.input=/path`

The CLI argument takes precedence, followed by environment variable and config sources.

## Customizing Input

Implement the `getInputMulti()` stub in the generated `OrchestratorApplication` to map inputs to domain types and feed the pipeline.
