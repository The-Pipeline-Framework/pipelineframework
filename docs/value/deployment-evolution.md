# Container and Kubernetes Ready

<p class="value-lead">The Pipeline Framework (TPF) generates standard Quarkus service runtimes that fit container and Kubernetes deployments, while still giving teams a practical path from one deployable to split layouts later.</p>

See the [Runtime Layouts guide](/guide/build/runtime-layouts/) for the packaging details: choosing a monolith-style runtime does not automatically remove per-step service modules, because build topology and runtime layout are separate decisions.

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Container-Friendly Runtime</strong> &middot; Use standard Quarkus service runtimes that fit normal container and Kubernetes operations.</div>
  <div class="value-glance-item"><strong>Fast First Release</strong> &middot; Start with one application when that is the fastest path.</div>
  <div class="value-glance-item"><strong>Clear Migration Path</strong> &middot; Move to grouped or modular layouts as service splits become clearer.</div>
  <div class="value-glance-item"><strong>No Core Rewrite</strong> &middot; Keep the typed business functions stable while runtime layout and build topology evolve.</div>
</div>

## Use This When

- You need speed now but know service ownership and deployment splits will evolve.
- Team ownership is outgrowing a single deployable unit.
- You want normal container or Kubernetes operations without giving up a later path to split runtimes.
- Architecture discussions are stalling delivery.

TPF separates **runtime layout** from **build topology**. Runtime layout is the logical shape of the running system: where the orchestrator, functions, and side effects live. Build topology is the Maven and container structure that physically produces deployables. Changing the runtime layout changes generated placement and calls; it does not automatically rewrite Maven modules.

## Jump to Guides

<div class="value-links">

- [Runtime Layouts](/guide/build/runtime-layouts/)
- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [POM vs Layout Matrix](/guide/build/runtime-layouts/pom-layout-matrix)

</div>
