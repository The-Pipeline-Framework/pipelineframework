---
search: false
---

# MCP and Template Generation

TPF's template-generation tooling is split across two repositories.

This repository owns the framework compiler and the generated schema contract. The [`tpf-mcp-bridge`](https://github.com/The-Pipeline-Framework/tpf-mcp-bridge) repository owns the MCP bridge and the template-generator snapshot used by MCP-driven scaffold generation.

## Source of Truth

| Surface | Owner |
| --- | --- |
| Pipeline semantic model and validation | `framework/deployment` in this repository |
| Generator-facing schema export | `META-INF/pipeline/pipeline-template-schema.json` from `framework/deployment` |
| MCP bridge server | `tpf-mcp-bridge` |
| Template generator snapshot | `tpf-mcp-bridge/template-generator-node` |
| Canvas/web UI | `web-ui` in this repository |

The generator-facing schema is exported from a built framework artifact and consumed by the bridge repository. Do not treat a vendored bridge schema as the authority when framework semantics change.

## When To Use It

Use MCP/template generation when you need:

- scaffold generation from a structured pipeline model,
- Codex or MCP-driven project creation,
- compatibility checks for generated Java/POM output,
- automated smoke paths for scaffolded examples.

For current advanced application design, keep YAML as the canonical model and use [Pipeline Compilation](/versions/v26.6.2/develop/pipeline-compilation/) to understand what the framework validates and generates.

## Release Coordination

The bridge release is coordinated with TPF framework releases but is not version-number synchronized. Framework releases publish the schema authority; bridge releases update the MCP/generator snapshot that consumes it.

When the schema or generated scaffold semantics change, validate both sides:

1. build `framework/deployment` so `pipeline-template-schema.json` is current,
2. refresh the bridge snapshot,
3. run bridge generator tests,
4. compile a generated scaffold inside a compatible TPF worktree.

## Related

- [Template Generator Reference](/versions/v26.6.2/evolve/template-generator)
- [Pipeline Compilation](/versions/v26.6.2/develop/pipeline-compilation/)
- [Pipeline Studio](/versions/v26.6.2/design/pipeline-studio/)

