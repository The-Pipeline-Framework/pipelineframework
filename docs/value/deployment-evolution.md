# Start Together, Split Deliberately

<p class="value-lead">Begin with the runtime shape that lets the team deliver, then move boundaries
when workload, ownership, or risk provides evidence—without pretending runtime layout and Maven
topology are the same decision.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>One Contract First</strong> &middot; Compose local services, nested pipelines, imported Blocks, and Connectors in one typed application model.</div>
  <div class="value-glance-item"><strong>Split on Evidence</strong> &middot; Move runtime boundaries when scaling, fault isolation, security, or ownership requires them.</div>
  <div class="value-glance-item"><strong>Build Topology Stays Explicit</strong> &middot; A logical layout change does not silently restructure Maven modules, JARs, or containers.</div>
  <div class="value-glance-item"><strong>Business Core Survives</strong> &middot; Generated callers and placement change around the same typed functions and effects.</div>
</div>

## Two Evolution Paths

```mermaid
flowchart LR
    P[One typed pipeline contract] --> L1[Local or grouped runtime]
    P --> B1[Existing build topology]
    L1 -->|workload or ownership evidence| L2[Split runtime layout]
    B1 -->|explicit packaging work| B2[Split build topology]
    L2 --> D[Deployable services or functions]
    B2 --> D
```

Runtime layout says where orchestrator, steps, and side effects execute. Build topology says which
modules and artefacts physically produce deployables. Teams may evolve them together, but TPF does
not claim that changing one YAML mapping automatically performs the other migration.

This distinction matters for modern TPF applications. A nested agent pipeline remains local
composition inside the root execution. A Block becomes ordinary linked pipeline definitions at
compile time. A Connector remains an external boundary. None of those concepts requires one
container per step, and none prevents an intentional service split later.

## Use This When

- a small team needs one deployable before it needs a platform programme;
- one capability has different scaling, security, or failure-isolation needs;
- team ownership is outgrowing a shared runtime;
- an AI or SaaS integration needs a host-specific connection boundary;
- architecture discussions are treating “microservices” or “monolith” as irreversible identities.

The Coffee Machine follows the argument through
[one pipeline, one container?](/architecture/coffee-machine/make-it-run/one-pipeline-one-container),
[runtime placement is a decision](/architecture/coffee-machine/make-it-run/runtime-placement-is-a-decision),
and
[no generated distributed monolith](/architecture/coffee-machine/make-it-run/no-generated-distributed-monolith).

## Go Deeper

<div class="value-links">

- [Runtime Layouts](/deploy/runtime-layouts/)
- [Using Runtime Mapping](/deploy/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/deploy/runtime-layouts/maven-migration)
- [POM vs Layout Matrix](/deploy/runtime-layouts/pom-layout-matrix)
- [Application Structure](/architecture/application-structure)

</div>
