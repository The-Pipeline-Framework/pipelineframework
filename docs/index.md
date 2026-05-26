---
layout: home

hero:
  name: The Pipeline Framework
  text: Fast Java Function Flows on Quarkus
  tagline: Write small typed business functions; TPF generates, validates, and reliably runs the Quarkus runtime around them
  image:
    src: /logo.png
    alt: The Pipeline Framework
  actions:
    - theme: alt
      text: Quick Start
      link: /guide/getting-started/
    - theme: brand
      text: Design with Canvas
      link: https://app.pipelineframework.org
---

<section class="home-cinematic-feature">
  <div class="home-cinematic-copy">
    <p class="home-cinematic-eyebrow">Await, provider, persistence, replay</p>
    <h2>Keep the business stages simple while TPF runs the async edges and durable state around them</h2>
    <p>
      This cinematic is derived from a real CSV Payments replay. It shows the flow crossing an await boundary,
      handing work to an external provider, resuming the primary path, and writing durable state without pushing
      that coordination into the business functions themselves.
    </p>
    <div class="home-cinematic-links">
      <a href="/replay-viewer/" class="home-cinematic-link primary">Open Replay Viewer</a>
      <a href="/guide/operations/observability/replay" class="home-cinematic-link secondary">Replay &amp; Live Topology</a>
    </div>
  </div>
  <div class="home-cinematic-media">
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
  </div>
</section>

<Callout type="tip" title="Fastest path: design with Canvas">
Use <a href="https://app.pipelineframework.org" target="_blank" rel="noopener noreferrer">Canvas</a> when you want to sketch the flow visually and download a runnable Quarkus scaffold. The same model can then be refined through YAML, Java functions, existing-method operators, type mappers, and runtime configuration.
</Callout>

<Callout type="info" title="Platform mode and transport mode are different decisions">
`COMPUTE` and `FUNCTION` are platform modes: they decide whether TPF generates a standard Quarkus service runtime for containers and Kubernetes or a function-style runtime. REST, gRPC, and local are transport modes: they decide how generated components call each other.
</Callout>

<FeaturedArticles />
<LatestReleases />

## Why Teams Choose TPF

<div class="home-capability-grid">
  <a class="home-capability-card" href="/value/developer-experience">
    <h3>Clear Inputs and Outputs</h3>
    <p>Each step is one Java function with explicit input and output types, so it is obvious what goes in and what comes out.</p>
  </a>
  <a class="home-capability-card" href="/guide/build/pipeline-compilation">
    <h3>Define the Flow in YAML</h3>
    <p>Describe the flow once, then let TPF generate the Quarkus endpoints, callers, handlers, and runtime files around it.</p>
  </a>
  <a class="home-capability-card" href="/guide/development/operators">
    <h3>Reuse Existing Java Code</h3>
    <p>Turn an existing Java method or remote endpoint into a pipeline function without hiding its inputs and outputs.</p>
  </a>
  <a class="home-capability-card" href="/guide/development/orchestrator-runtime">
    <h3>Reliable Background Work</h3>
    <p>Store accepted work, retry failed transitions, recover after crashes, and send terminal failures to a dead-letter channel.</p>
  </a>
  <a class="home-capability-card" href="/value/runtime-efficiency">
    <h3>Runtime Efficiency</h3>
    <p>Keep many items moving through reactive function chains without one thread per item.</p>
  </a>
  <a class="home-capability-card" href="/value/integration-flexibility">
    <h3>Portable Serverless Functions</h3>
    <p>Generate serverless function entry points and handlers for AWS Lambda, Azure Functions, and Google Cloud Run functions from the same typed Java flow.</p>
  </a>
  <a class="home-capability-card" href="/value/deployment-evolution">
    <h3>Container and Kubernetes Ready</h3>
    <p>Generate standard Quarkus service runtimes for containers and Kubernetes, then evolve from monolith to split layouts when needed.</p>
  </a>
  <a class="home-capability-card" href="/value/state-replay-and-queryable-data">
    <h3>State, Replay, and Queryable Data</h3>
    <p>Use persistence for durable business state and caching for fast recomputation, replay, and query-ready outputs.</p>
  </a>
</div>

## What TPF Helps You Build

TPF pipelines are fast, forward-only flows of Java functions. They are not CI/CD job chains or coarse batch jobs. A **step** is one function in the flow: it receives a typed Java input, produces a typed Java output, and leaves the repeated Quarkus code to TPF.

<div class="home-proof-grid">
  <div>
    <h3>What you write</h3>
    <p>Focused Java business functions such as validating a payment, enriching a record, parsing a document, or calling an existing Java method.</p>
  </div>
  <div>
    <h3>What TPF generates</h3>
    <p>REST, gRPC, local, and function-style code that calls those functions, plus runtime files that tell the generated runtime which function comes next.</p>
  </div>
  <div>
    <h3>What TPF runs</h3>
    <p>The generated runner that starts the flow, calls each step, tracks progress when configured, retries failed work, and routes failures.</p>
  </div>
</div>

## How It Works

### Write functions, not glue

You write typed Java functions and declare their order in YAML. TPF checks the flow at build time, including whether each function shape is valid, whether outputs match the next inputs, whether [mappers](/guide/development/mappers-and-dtos) convert types correctly, whether [operators](/guide/development/operators) are valid, and whether the generated code can call each function cleanly.

A **mapper** translates between your domain types and transport or external-system types. An **operator** is an existing Java method or remote endpoint reused as a pipeline function.

Start with [pipeline compilation](/guide/build/pipeline-compilation) when you want the build-time generation model, or [code a step](/guide/development/code-a-step) when you want to implement the function itself.

### Generate the repeated Quarkus code

TPF creates the REST resources, gRPC services, local clients, function-style handlers, and runtime files that would otherwise become handwritten service glue. An **adapter** is the generated code around your business function: it lets another component call the function through the selected transport.

Generated code keeps business logic independent of whether a caller uses REST, gRPC, local in-process calls, or a serverless function entry point. That same generated runtime can be packaged as a standard Quarkus service for containers and Kubernetes or as a `FUNCTION` deployment when that model fits better. See [Portable Serverless Functions](/value/integration-flexibility) and [runtime layouts](/guide/build/runtime-layouts/) when you want the details behind platform mode, transport mode, logical placement, and Maven/container packaging.

### Run the flow reliably

The generated runtime starts the flow, invokes each step, records execution state when configured, retries failed transitions, and exposes status/result endpoints for background work. For crash-surviving background execution, TPF can store accepted work outside the current JVM, recover it after restart, and send terminal execution failures to a **DLQ**, a dead-letter channel for investigation or replay.

The exact config value for this mode is `QUEUE_ASYNC`; start with the [Orchestrator Runtime](/guide/development/orchestrator-runtime) guide before using it in production.

Persistence and caching matter here more than the word "plugin" suggests. Persistence stores business outputs developers can query later from APIs, reports, or UIs. Caching protects expensive steps, speeds recomputation, and supports replay or rewind scenarios when downstream logic changes. Together they reduce the need to invent separate state stores, replay workflows, or read-model plumbing.

<Callout type="info" title="High availability is a runtime responsibility">
TPF does not just generate code and walk away. In the background execution path, it owns stored execution state, dispatch, retry, crash recovery, and terminal failure publication. Your team still owns provider choice, duplicate-protection policy, retry budgets, observability thresholds, and deployment rollout.
</Callout>

### Add cross-cutting work without hiding it

[Plugins](/guide/development/using-plugins) add declared cross-cutting work such as persistence, cache, telemetry, and logging. An **aspect** is the rule that says where the plugin runs, for example before or after a step; the plugin provides the implementation.

That keeps business functions focused on domain behaviour while TPF keeps generated plugin calls aligned across REST, gRPC, and local execution.

See [State, Replay, and Queryable Data](/value/state-replay-and-queryable-data) for the full persistence-and-caching story.

<Callout type="tip" title="Operators preserve existing investment">
If you already have proven Java libraries or remote endpoints, operators let you reuse them as pipeline functions. TPF still validates the method reference, input/output types, type-translation compatibility, and generated function call instead of turning reuse into hidden service glue.
</Callout>

## What This Looks Like in Practice

<div class="home-proof-grid">
  <div>
    <h3>Payments</h3>
    <p>Validate and enrich records, produce status output, reject one malformed item, and continue the workload.</p>
  </div>
  <div>
    <h3>Search</h3>
    <p>Crawl, parse, tokenize, index, cache intermediate results, and replay safely when source content changes.</p>
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
    <h3>Serverless deployments</h3>
    <p>Run the same typed flow behind AWS Lambda, Azure Functions, or Google Cloud Run functions without rewriting the business code.</p>
  </div>
  <div>
    <h3>Container platforms</h3>
    <p>Run the same typed flow as a standard Quarkus service in containers or Kubernetes when you want service-style deployment and operations.</p>
  </div>
</div>

## Start Here

- [Quick Start](/guide/getting-started/) for the fastest path to a runnable scaffold.
- [Pipeline Compilation](/guide/build/pipeline-compilation) for YAML-first generation and build-time validation.
- [Operators](/guide/development/operators) for reusing existing Java methods or remote endpoints.
- [Runtime Layouts](/guide/build/runtime-layouts/) for logical placement versus Maven/container build topology.
- [AWS Lambda Platform](/guide/development/aws-lambda) for the canonical `FUNCTION` platform guide.
- [Azure Functions Platform](/guide/development/azure-functions) for Azure development-time guidance.
- [Google Cloud Run Functions Platform](/guide/development/google-cloud-run-functions) for the Google function-platform path.
- [Multi-Cloud Function Providers](/guide/build/runtime-layouts/function-providers) for provider-specific function deployment targets.
- [Orchestrator Runtime](/guide/development/orchestrator-runtime) for synchronous execution, background execution, crash recovery, and DLQ behaviour.
- [Using Plugins](/guide/development/using-plugins) for persistence, cache, telemetry, and logging extensions.
- [TPFGo Example](/guide/development/tpfgo-example) for checkpoint handoff between pipelines.
