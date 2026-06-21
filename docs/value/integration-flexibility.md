# Portable Serverless Functions

<p class="value-lead">The Pipeline Framework (TPF) lets teams keep the same typed Java functions while targeting either a standard Quarkus service runtime (`COMPUTE`) or a portable serverless function runtime (`FUNCTION`).</p>

## At a Glance

<div class="value-glance">
  <div class="value-glance-item"><strong>Same Functions, Different Runtime</strong> &middot; Keep the same typed Java flow while choosing between `COMPUTE` and `FUNCTION` platform modes.</div>
  <div class="value-glance-item"><strong>Portable Serverless Targets</strong> &middot; Target AWS Lambda, Azure Functions, and Google's Cloud Run functions without rewriting the business functions.</div>
  <div class="value-glance-item"><strong>Transport Still Separate</strong> &middot; REST, gRPC, and local calls remain a separate transport decision from the platform mode.</div>
</div>

## Use This When

- You want a serverless deployment target without rewriting the business flow.
- You need the same pipeline logic to stay portable across cloud function providers.
- You need platform choices and call mechanisms to stay explicit instead of getting mixed together.

TPF keeps the business functions stable while changing the generated runtime around them. That means you can keep one typed Java flow and choose whether TPF generates a standard service runtime or serverless function entry points.

## Platform mode vs transport mode

These are different decisions:

1. **Platform mode** chooses the generated runtime shape.
   - `COMPUTE`: standard Quarkus service/runtime
   - `FUNCTION`: serverless function entry points and handlers
2. **Transport mode** chooses how generated components call each other.
   - REST
   - gRPC
   - local in-process calls

In TPF, an **adapter** is generated code that lets another component call your business function. Platform mode decides what kind of runtime TPF generates around that function. Transport mode decides how the generated components talk.

## What FUNCTION mode gives you

With `FUNCTION` mode, TPF can generate serverless function entry points while preserving the same typed Java functions and build-time validation rules.

This is the current portable function-platform story:

1. AWS Lambda
2. Azure Functions
3. Google's Cloud Run functions

The provider-specific handler shape changes, but the flow logic, operator reuse, and generated validation model stay aligned.

## Current constraints

Keep the current platform limits explicit:

1. `FUNCTION` currently requires `REST` transport; `gRPC` is not a supported `FUNCTION` transport today.
2. Checkpoint handoff is not available in `FUNCTION` mode.
3. Queue-backed HA and crash recovery belong to the orchestrator `COMPUTE` + `QUEUE_ASYNC` path. `FUNCTION` mode is limited to generated serverless entry points and provider portability; it does not implement the queue-async recovery model.

## Jump to Guides

<div class="value-links">

- [AWS Lambda Platform](/deploy/aws-lambda)
- [Azure Functions Platform](/deploy/azure-functions)
- [Google Cloud Run Functions Platform](/deploy/google-cloud-run-functions)
- [Runtime Layouts](/deploy/runtime-layouts/)
- [Multi-Cloud Function Providers](/deploy/function-providers)
- [Orchestrator Runtime](/deploy/orchestrator-runtime/)

</div>
