# Transport Choice

<p class="value-lead">TPF keeps business functions separate from how they are called, so APIs can evolve without rewriting core flow logic.</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>One Flow, Many Entry Points</strong> &middot; Generate gRPC, REST, local, and function-style callers from the same definition.</div>
  <div class="value-glance-item"><strong>Domain First</strong> &middot; Protocol details stay in generated code, not business functions.</div>
  <div class="value-glance-item"><strong>Function Ready</strong> &middot; Run through function-style entry points where that deployment model fits.</div>
</div>

## Use This When

- Different clients need different protocols.
- You are migrating interfaces and need to avoid big-bang rewrites.
- You need Java types and API shapes to stay consistent without manual reconciliation.

In TPF, **transport mode** means how generated components call each other: gRPC, REST, or local in-process calls. It is separate from where the application is deployed.

## Jump to Guides

<div class="value-links">

- [Customization points](/guide/development/customization-points)
- [AWS Lambda Platform](/guide/development/aws-lambda)
- [Runtime Layouts](/guide/build/runtime-layouts/)

</div>
