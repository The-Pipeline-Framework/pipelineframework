# Bridge Release Coordination

The Java framework and `tpf-mcp-bridge` are related but not version-number synchronized.

## Ownership

| Surface | Owner |
| --- | --- |
| Semantic pipeline model | this repository, `framework/deployment` |
| Generator-facing schema export | this repository, `META-INF/pipeline/pipeline-template-schema.json` |
| MCP bridge server | `The-Pipeline-Framework/tpf-mcp-bridge` |
| Node template generator snapshot | `tpf-mcp-bridge/template-generator-node` |

## When To Coordinate

Coordinate a bridge release when:

- the schema changes,
- generated scaffold semantics change,
- Canvas/template generation contracts change,
- MCP bridge behavior depends on a new framework release.

## Validation Shape

1. Build `framework/deployment` so the schema export is current.
2. Refresh the bridge snapshot from the built framework artifact.
3. Run bridge generator tests in the bridge repository.
4. Compile a generated scaffold against the intended framework version.

For user-facing context, see [MCP and Template Generation](/develop/mcp-template-generation).

