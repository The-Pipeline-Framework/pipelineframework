# Start Monolith, Split Later

<p class="value-lead">The Pipeline Framework (TPF) supports a practical architecture path: start with one deployable, then split when team ownership or scale makes it worth it.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Fast First Release</strong> &middot; Start with one application when that is the fastest path.</div>
  <div class="value-glance-item"><strong>Clear Migration Path</strong> &middot; Move to grouped or modular layouts as boundaries mature.</div>
  <div class="value-glance-item"><strong>No Core Rewrite</strong> &middot; Change deployable shape without rewriting the business functions.</div>
</div>

## Use This When

- You need speed now but know service boundaries will evolve.
- Team ownership is outgrowing a single deployable unit.
- Architecture discussions are stalling delivery.

TPF separates **runtime layout** from **build topology**. Runtime layout is where the orchestrator, functions, and side effects logically run. Build topology is the Maven/container structure that physically creates deployables.

## Jump to Guides

<div class="value-links">

- [Runtime Layouts](/guide/build/runtime-layouts/)
- [Using Runtime Mapping](/guide/build/runtime-layouts/using-runtime-mapping)
- [Maven Migration Playbook](/guide/build/runtime-layouts/maven-migration)
- [POM vs Layout Matrix](/guide/build/runtime-layouts/pom-layout-matrix)

</div>
