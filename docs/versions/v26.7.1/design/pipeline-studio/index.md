---
search: false
---

# Quick Start

Pipeline Studio is the visual Canvas path for sketching a baseline TPF application. It is useful for first-pass flow modelling and scaffold generation, but YAML remains the canonical path for current advanced features.

::: warning Current Scope
Pipeline Studio does not yet expose every current TPF capability. Use YAML and the linked guides for await boundaries, object ingest, checkpoint handoff, runtime mapping, advanced configuration, and Spring portability work.
:::

<Callout type="tip" title="Prerequisites">
Before you begin, ensure you have Java 21+, Maven 3.8+, and an IDE installed on your system for building and running generated applications. Quarkus is the mature generated runtime today.
</Callout>

## Quick Start with Canvas

Open the visual Canvas designer at [https://app.pipelineframework.org](https://app.pipelineframework.org).

### 1. Access the Canvas Designer

Visit [https://app.pipelineframework.org](https://app.pipelineframework.org) in your web browser.

### 2. Create Your First Pipeline

1. Click the "New Pipeline" button
2. Start adding steps by clicking the "+" button
3. Configure each step by:
   - Setting a descriptive name
   - Choosing the appropriate cardinality (1-1, Expansion, Reduction, or Side effect)
   - Defining input and output fields with rich Java types

### 3. Download Your Complete Application

Once you've designed your pipeline:
1. Click the "Download Application" button
2. Save the generated ZIP file containing your complete application

### 4. Build and Run Your Application

Extract the ZIP file and navigate to the application directory:

```bash
unzip your-pipeline-app.zip
cd your-pipeline-app
./mvnw clean compile
./mvnw quarkus:dev
```

Your application is now running locally.

## Next Steps

After you have a baseline, use the current guides for the parts Canvas does not fully model yet:

- [Functional Core, Imperative Shell](/versions/v26.7.1/design/fcis)
- [Application Structure](/versions/v26.7.1/design/application-structure)
- [Pipeline Compilation](/versions/v26.7.1/develop/pipeline-compilation/)
- [Configuration](/versions/v26.7.1/develop/configuration/)
- [Object Ingest](/versions/v26.7.1/design/object-ingest)
- [Await Boundaries](/versions/v26.7.1/design/await-boundaries)
- [Checkpoint Handoff](/versions/v26.7.1/deploy/orchestrator-runtime/checkpoint-handoff)

## Template Generator (Reference Only)

Use [MCP and Template Generation](/versions/v26.7.1/develop/mcp-template-generation) when you need automation or CI-driven scaffold generation.

See [Template Generator Reference](/versions/v26.7.1/evolve/template-generator) for the lower-level reference.
