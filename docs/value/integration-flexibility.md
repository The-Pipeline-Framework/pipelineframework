# Transport Choice

<p class="value-lead">TPF keeps business functions separate from how they are called, so REST, gRPC, local, and cloud-function entry points can evolve without rewriting core flow logic.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>One Flow, Many Entry Points</strong> &middot; Generate gRPC, REST, local, and cloud-function callers from the same YAML definition.</div>
  <div class="value-glance-item"><strong>Domain First</strong> &middot; Protocol details stay in generated caller code, not business functions.</div>
  <div class="value-glance-item"><strong>Function Ready</strong> &middot; Run the same business code through cloud-function entry points when that deployment model fits.</div>
</div>

## Use This When

- Different clients need different protocols.
- You are migrating interfaces and need to avoid big-bang rewrites.
- You need Java types and API shapes to stay consistent without manual reconciliation.

In TPF, an **adapter** is generated code that lets another component call your business function. **Transport mode** means how generated components call each other: gRPC, REST, or local in-process calls. It is separate from the deployment style, which decides whether the runtime behaves like a normal Quarkus service or a cloud function.

## Jump to Guides

<div class="value-links">

- [Customization points](/guide/development/customization-points)
- [AWS Lambda Platform](/guide/development/aws-lambda)
- [Runtime Layouts](/guide/build/runtime-layouts/)
- [Multi-Cloud Function Providers](/guide/build/runtime-layouts/function-providers)

</div>
