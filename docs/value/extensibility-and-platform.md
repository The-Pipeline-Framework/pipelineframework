# Plugins, Not Glue

<p class="value-lead">TPF gives teams structured extension points so infrastructure concerns do not leak into business functions.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Clean Extensions</strong> &middot; Add persistence, caching, telemetry, and logging as declared plugins.</div>
  <div class="value-glance-item"><strong>Consistent Integration</strong> &middot; Generated callers keep plugin behaviour aligned across REST, gRPC, and local paths.</div>
  <div class="value-glance-item"><strong>Strong Platform Base</strong> &middot; Quarkus runtime and tooling stay available underneath TPF.</div>
</div>

## Use This When

- Platform features keep duplicating across teams.
- Core services are getting polluted with infrastructure logic.
- You need shared extension patterns with predictable behaviour.

In TPF, an **aspect** says where a plugin should run, such as before or after a step. A **plugin** provides the implementation, such as persistence or cache. Your business function remains focused on the domain work.

## Jump to Guides

<div class="value-links">

- [Using Plugins](/guide/development/using-plugins)
- [Writing a Plugin](/guide/plugins/writing-a-plugin)
- [Persistence Plugin](/guide/plugins/persistence)
- [Plugins Architecture](/guide/evolve/plugins-architecture)

</div>
