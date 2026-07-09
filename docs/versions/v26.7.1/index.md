---
layout: home

hero:
  name: The Pipeline Framework
  text: Keep the core pure. Connect to reality.
  tagline: "Not another workflow engine or CI/CD pipeline. TPF pipelines are strongly typed application flows: each step transforms explicit business types while TPF handles persistence, transport, replay, observability, retries, and deployment concerns."
  image:
    src: /logo.png
    alt: The Pipeline Framework
  actions:
    - theme: alt
      text: Quick Start
      link: /versions/v26.7.1/design/pipeline-studio/
    - theme: brand
      text: Design with Canvas
      link: https://app.pipelineframework.org
search: false
---

<figure class="home-cinematic-spotlight">
  <a class="home-cinematic-video-link" href="/replay-viewer/" aria-label="Open the replay viewer">
    <video
      class="home-cinematic-video"
      autoplay
      muted
      loop
      playsinline
      preload="metadata"
      poster="/home/replay-proof-poster.jpg"
    >
      <source src="/home/replay-proof.webm" type="video/webm" />
      <source src="/home/replay-proof.mp4" type="video/mp4" />
    </video>
  </a>
  <figcaption>Async application flow: an await step hands work through Kafka to an external provider, then resumes from captured state with deterministic lineage.</figcaption>
</figure>

<Callout type="tip" title="Fastest path: design with Canvas">
Use <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">Canvas</a> when you want to model a typed application flow visually and download a runnable baseline. The same model can then be refined through YAML, Java functions, existing-method operators, type mappers, connectors, and runtime configuration.
</Callout>

<Callout type="info" title="TPF understands contracts, not just order">
The framework understands the contracts flowing through the system, not just the order of execution, so teams can evolve from local execution to APIs, messaging, serverless platforms, and distributed deployments without rewriting business logic.
</Callout>

<FeaturedArticles />
<LatestReleases />

## Why Teams Choose TPF

<div class="home-capability-grid">
  <a class="home-capability-card" href="/versions/v26.7.1/develop/pipeline-compilation/">
    <h3>Strongly Typed Pipelines</h3>
    <p>Model business flows as explicit transformations between typed contracts. TPF validates application boundaries, cardinality, mappings, and transport compatibility before deployment, reducing semantic drift as systems evolve.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/develop/code-a-step">
    <h3>Keep the Core Pure</h3>
    <p>Write business logic as ordinary Java functions. TPF isolates persistence, messaging, APIs, file ingestion, asynchronous workflows, retries, and deployment concerns from application decisions.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/design/object-ingest">
    <h3>Connect to Reality</h3>
    <p>Databases, APIs, Kafka, S3, files, human approvals, long-running jobs, and external providers are modeled through connectors and runtime primitives instead of custom glue code.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/deploy/runtime-layouts/">
    <h3>Transport Independence</h3>
    <p>Run locally, over REST, gRPC, Kafka, SQS, or serverless platforms without changing business logic. Infrastructure choices become deployment concerns rather than application concerns.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/value/state-replay-and-queryable-data">
    <h3>Replayable by Design</h3>
    <p>Execution lineage, captured external state, persistence, caching, retries, and idempotency help make distributed systems easier to reason about, debug, and recover.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/operate/observability/">
    <h3>Operational Visibility Built In</h3>
    <p>Execution tracking, telemetry, audit trails, checkpointing, and runtime validation are available from the beginning rather than added later through custom instrumentation.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/design/await-boundaries">
    <h3>Await External Reality</h3>
    <p>Pause execution while waiting for human approvals, webhook callbacks, provider responses, and long-running jobs without polling loops, correlation tables, or custom state machines.</p>
  </a>
  <a class="home-capability-card" href="/versions/v26.7.1/develop/mappers-and-dtos">
    <h3>Data Fails First</h3>
    <p>Distributed-system complexity often begins as semantic drift rather than network failures. TPF makes contracts explicit through strongly typed application boundaries and generated transports, helping teams evolve safely as systems grow.</p>
  </a>
</div>

## What TPF Helps You Build

TPF pipelines are strongly typed application flows. They are not CI/CD job chains, coarse batch jobs, or generic workflow diagrams. A **step** is one business transformation: it receives an explicit Java input type, produces an explicit Java output type, and leaves distributed-system concerns to the framework-owned shell.

<div class="home-proof-grid">
  <div>
    <h3>Functional core</h3>
    <p>Focused Java business functions such as validating a payment, enriching a record, parsing a document, or deciding how to handle a domain event.</p>
  </div>
  <div>
    <h3>I/O shell</h3>
    <p>Connectors, adapters, mappers, persistence, caching, await boundaries, and transport-specific calls that connect the core to external reality.</p>
  </div>
  <div>
    <h3>Runtime proof</h3>
    <p>The runtime starts the flow, calls each step, records lineage when configured, retries failed work, exposes status, and routes terminal failures.</p>
  </div>
</div>

## How It Works

### Keep the core pure

You write typed Java functions and declare their application flow in YAML. TPF checks the flow at build time, including whether each function shape is valid, whether outputs match the next inputs, whether [mappers](/versions/v26.7.1/develop/mappers-and-dtos) convert types correctly, whether [operators](/versions/v26.7.1/develop/operators) are valid, and whether generated transports can call each boundary cleanly.

A **mapper** translates between your domain types and transport or external-system types. An **operator** is an existing Java method or remote endpoint reused as a pipeline function.

Start with [pipeline compilation](/versions/v26.7.1/develop/pipeline-compilation/) when you want the build-time generation model, or [code a step](/versions/v26.7.1/develop/code-a-step) when you want to implement the function itself.

### Connect to reality through explicit shells

TPF creates the connector and transport shells that would otherwise become handwritten service glue. An **adapter** is generated code around your business function: it lets another component call the function through the selected transport without moving transport logic into the function.

Connectors own I/O admission such as file and object ingest. Await steps own durable suspend/resume for human approvals, webhook callbacks, provider responses, and long-running jobs. Generated transports keep business logic independent of whether a caller uses local calls, REST, gRPC, messaging, or a serverless function entry point. See [Object Ingest](/versions/v26.7.1/design/object-ingest), [Await Boundaries](/versions/v26.7.1/design/await-boundaries), and [runtime layouts](/versions/v26.7.1/deploy/runtime-layouts/) when you want the implementation details.

### Run the flow reliably

The runtime starts the flow, invokes each step, records execution state when configured, retries failed transitions, and exposes status/result endpoints for background work. For crash-surviving background execution, TPF can store accepted work outside the current JVM, recover it after restart, and send terminal execution failures to a **DLQ**, a dead-letter channel for investigation or replay.

The exact config value for this mode is `QUEUE_ASYNC`; start with the [Orchestrator Runtime](/versions/v26.7.1/deploy/orchestrator-runtime/) guide before using it in production.

Persistence and caching matter here more than the word "plugin" suggests. Persistence stores business outputs developers can query later from APIs, reports, or UIs. Caching protects expensive steps, speeds recomputation, and supports replay or rewind scenarios when downstream logic changes. Together they reduce the need to invent separate state stores, replay workflows, or read-model plumbing.

<Callout type="info" title="Infrastructure is still your decision">
TPF keeps transport, platform, and deployment choices outside the business functions. Your team still owns provider choice, duplicate-protection policy, retry budgets, observability thresholds, and deployment rollout.
</Callout>

### Add cross-cutting work without hiding it

[Plugins](/versions/v26.7.1/develop/using-plugins) add declared cross-cutting work such as persistence, cache, telemetry, and logging. An **aspect** is the rule that says where the plugin runs, for example before or after a step; the plugin provides the implementation.

That keeps business functions focused on domain behaviour while TPF keeps generated plugin calls aligned across transports and runtime layouts.

See [State, Replay, and Queryable Data](/versions/v26.7.1/value/state-replay-and-queryable-data) for the full persistence-and-caching story.

<Callout type="tip" title="Operators preserve existing investment">
If you already have proven Java libraries or remote endpoints, operators let you reuse them as pipeline functions. TPF still validates the method reference, input/output types, type-translation compatibility, and generated function call instead of turning reuse into hidden service glue.
</Callout>

## What This Looks Like in Practice

<div class="home-proof-grid">
  <div>
    <h3>Payments</h3>
    <p>Validate and enrich records, await provider decisions through Kafka, produce status output, reject one malformed item, and continue the workload.</p>
  </div>
  <div>
    <h3>Search</h3>
    <p>Admit documents through object connectors, parse, tokenize, index, cache intermediate results, and replay safely when source content changes.</p>
  </div>
  <div>
    <h3>Checkout and TPFGo</h3>
    <p>Pass stable checkpoints from one pipeline to the next without inventing custom handoff code.</p>
  </div>
  <div>
    <h3>AI enrichment</h3>
    <p>Reuse embedding, vector-search, or LLM helper libraries as operators inside a typed Java flow.</p>
  </div>
  <div>
    <h3>External approvals</h3>
    <p>Pause while a person, webhook, provider, or long-running job decides, then resume the same execution without custom correlation state.</p>
  </div>
  <div>
    <h3>Deployment options</h3>
    <p>Run the same typed flow locally, behind APIs, through messaging, on serverless platforms, or as distributed services when the deployment model requires it.</p>
  </div>
</div>

## Start Here

- [Quick Start](/versions/v26.7.1/design/pipeline-studio/) for the fastest path to a runnable scaffold.
- [Pipeline Compilation](/versions/v26.7.1/develop/pipeline-compilation/) for YAML-first generation and build-time validation.
- [Operators](/versions/v26.7.1/develop/operators) for reusing existing Java methods or remote endpoints.
- [Runtime Layouts](/versions/v26.7.1/deploy/runtime-layouts/) for logical placement versus Maven/container build topology.
- [AWS Lambda Platform](/versions/v26.7.1/deploy/aws-lambda) for the canonical `FUNCTION` platform guide.
- [Azure Functions Platform](/versions/v26.7.1/deploy/azure-functions) for Azure development-time guidance.
- [Google Cloud Run Functions Platform](/versions/v26.7.1/deploy/google-cloud-run-functions) for the Google function-platform path.
- [Multi-Cloud Function Providers](/versions/v26.7.1/deploy/function-providers) for provider-specific function deployment targets.
- [Orchestrator Runtime](/versions/v26.7.1/deploy/orchestrator-runtime/) for synchronous execution, background execution, crash recovery, and DLQ behaviour.
- [Using Plugins](/versions/v26.7.1/develop/using-plugins) for persistence, cache, telemetry, and logging extensions.
- [TPFGo Example](/versions/v26.7.1/develop/tpfgo-example) for checkpoint handoff between pipelines.
