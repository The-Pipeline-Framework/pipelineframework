---
search: false
---

# Maven And Scaffolding

Generated POMs and framework artifacts are Quarkus-first today. That is acceptable for current users but limits Spring-first scaffolding.

Proposed target artifacts:

- `tpf-api`
- `tpf-compiler-core`
- `tpf-runtime-core`
- `tpf-runtime-mutiny`
- `tpf-runtime-reactor`
- `tpf-quarkus-extension`
- `tpf-spring-boot-starter`

Spring scaffolds are likely simpler for local/REST and harder for build-time generation unless a plugin-based compiler flow is part of the adapter story.
