---
search: false
---

# Plugins, Not Glue

<p class="value-lead">TPF gives teams clear extension rules so infrastructure concerns do not leak into business functions.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Clean Extensions</strong> &middot; Add persistence, caching, telemetry, and logging as declared plugins.</div>
  <div class="value-glance-item"><strong>Consistent Integration</strong> &middot; Generated callers keep plugin behaviour aligned across REST, gRPC, and local paths.</div>
  <div class="value-glance-item"><strong>Explicit Rules</strong> &middot; Aspect rules say where cross-cutting work runs instead of hiding it inside business functions.</div>
</div>

## Use This When

- Platform features keep duplicating across teams.
- Core services are getting polluted with infrastructure logic.
- You need shared extension patterns with predictable behaviour.

In TPF, an **aspect** is the rule that says where a plugin should run, such as before or after a step. A **plugin** provides the implementation, such as persistence or cache. Your business function remains focused on the domain work.

Persistence and caching are more than generic extension examples. They are the main state-and-replay primitives in TPF: persistence keeps durable business records, and cache accelerates recomputation and replay. See the dedicated value page for that combined story.

## Jump to Guides

<div class="value-links">

- [Using Plugins](/versions/v26.7.1/develop/using-plugins)
- [State, Replay, and Queryable Data](/versions/v26.7.1/value/state-replay-and-queryable-data)
- [Writing a Plugin](/versions/v26.7.1/develop/writing-a-plugin)
- [Persistence Plugin](/versions/v26.7.1/design/persistence)
- [Plugins Architecture](/versions/v26.7.1/evolve/plugins-architecture)

</div>
