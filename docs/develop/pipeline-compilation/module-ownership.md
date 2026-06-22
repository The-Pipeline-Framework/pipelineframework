# Module Ownership

## Module Ownership and Dependencies

This build is organized into three groups plus shared scaffolding:

- `common` (scaffolded): DTOs and mappers used by services and orchestrator
- `services`: step implementations + server adapters (gRPC or REST)
- `orchestrator`: orchestrator endpoints + orchestrator CLI + client steps

Client steps are *only* used by the orchestrator (there is no direct step-to-step communication).

### Ownership Matrix (Generated Artifacts)

| Artifact | Owner Module | Consumers |
| --- | --- | --- |
| DTOs + mappers | `common` | `services`, `orchestrator` |
| gRPC server adapters | `services` | runtime/CDI |
| REST resources | `services` | runtime/CDI |
| gRPC client steps | `orchestrator` | orchestrator runtime |
| REST client steps | `orchestrator` | orchestrator runtime |
| Orchestrator endpoints | `orchestrator` | runtime/CDI |
| Orchestrator CLI | `orchestrator` | user |
