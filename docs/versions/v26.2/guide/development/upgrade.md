---
search: false
---

# Upgrade Guide

## Descriptor-Driven gRPC Type Resolution

If you are upgrading to a version that requires descriptor-driven gRPC resolution, you must enable descriptor set generation in your gRPC/protobuf build. Without it, gRPC service/client generation will fail at build time.

Ensure your build emits a descriptor set (for example via Quarkus gRPC codegen for your version), or provide
`protobuf.descriptor.path`/`protobuf.descriptor.file` to the annotation processor.

See [protobuf descriptor integration](/versions/v26.2/guide/evolve/protobuf-integration-descriptor-res) for full setup details and troubleshooting.

## Classifier Packaging Migration (Role Directories)

This migration assumes a baseline with no plugins/aspects and no classifier infrastructure. The goal is to introduce role-based packaging without a custom Maven plugin or class-name heuristics.

## Target State
- Generated sources are written to:
  - `target/generated-sources/pipeline/orchestrator-client`
  - `target/generated-sources/pipeline/pipeline-server`
  - `target/generated-sources/pipeline/plugin-client`
  - `target/generated-sources/pipeline/plugin-server`
  - `target/generated-sources/pipeline/rest-server`
- Each role directory is compiled into a role-specific output directory.
- `maven-jar-plugin` produces one classifier JAR per role using `classesDirectory`.
- `META-INF/pipeline/roles.json` may remain for documentation/validation only.

## Migration Steps

1. **Configure the annotation processor output root**
   - Add `-Apipeline.generatedSourcesDir=target/generated-sources/pipeline` to the compiler plugin.
   - Optional: set `<generatedSourcesDirectory>` to the same path for consistency.
   - If you see warnings during `testCompile`, scope this compiler arg to the `compile` execution only.

2. **Register role directories as sources (IDE visibility)**
   - Add `build-helper-maven-plugin` to register each role directory as a source root.
   - If tests reference generated classes, also register the same directories as test sources in `generate-test-sources`.

3. **Compile each role directory**
   - Add `maven-compiler-plugin` executions that compile each role directory to a dedicated output directory (e.g., `target/classes-pipeline/orchestrator-client`).
   - Disable annotation processing in these secondary executions with `<proc>none</proc>`.

4. **Package classifier JARs**
   - Use `maven-jar-plugin` executions with `classesDirectory` pointing to each role output directory.

## Benefits
- No class-name heuristics
- No custom Maven plugin
- Clear, deterministic packaging inputs
- Easier to extend with new roles
