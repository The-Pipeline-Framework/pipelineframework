# Examples Guide

TPF examples are compatibility surfaces: each one proves a particular combination of authoring,
generated code, runtime behaviour, or deployment shape. Start with the smallest example that matches
the question you are trying to answer; do not copy the largest topology by default.

```mermaid
flowchart LR
    Q{What do you need to prove?}
    Q -->|One contract| P[Focused proof]
    Q -->|A complete application| A[Application example]
    Q -->|HA or provider topology| H[Self-hosted reference]
    P --> R[Read its README and tests]
    A --> R
    H --> R
```

The [example catalogue](./catalogue) links every README under `examples/` and explains what it is
for. A few useful starting points are:

- [Stdio Object Demo](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/examples/stdio-object-demo/README.md) for the smallest generated object-I/O pipeline;
- [Restaurant Approval](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/examples/restaurant-approval/README.md) for a typed Await interaction;
- [Turnkey RAG](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/examples/rag-turnkey/README.md) for two independently deployable applications using Blocks, Query, Command, object ingest, and vector storage;
- [CSV Payments](https://github.com/The-Pipeline-Framework/pipelineframework/blob/main/examples/csv-payments/README.md) for the broadest runtime, telemetry, replay, and deployment compatibility surface.

Examples prove the checked-out version of the repository. For an application pinned to a released
TPF version, use the matching tag rather than assuming `main` has the same contract.
