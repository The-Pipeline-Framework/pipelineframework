# Publishing

Publishing TPF has three related but separate responsibilities:

1. publish Java framework artifacts to Maven Central,
2. snapshot the docs site for the released version,
3. coordinate the `tpf-mcp-bridge` release when schema or generator behavior changed.

Use this page as the release front door. The older full procedure remains available as [Publishing Reference](/evolve/publishing-reference).

## Release Path

| Need | Page |
| --- | --- |
| Cut and publish framework artifacts | [Framework Release Process](/evolve/framework-release-process) |
| Validate docs snapshots and route rewrites | [Docs Snapshot Process](/evolve/docs-snapshot-process) |
| Coordinate MCP bridge and template generator releases | [Bridge Release Coordination](/evolve/bridge-release-coordination) |
| Troubleshoot Maven Central details | [Publishing Reference](/evolve/publishing-reference) |

## Guardrails

- Do not push release commits or tags until local validation passes.
- Keep Maven Central publishing tied to immutable tags.
- Keep alternate topology POMs, standalone POMs, and docs snapshots aligned with the release.
- Treat `tpf-mcp-bridge` as coordinated but separately versioned.
