# Configuration (Overview)

This page maps the most-used configuration entry points and where to manage them for TPF applications.

## Primary References

- [Configuration Reference](/guide/build/configuration/) for full build-time and runtime key catalog
- [Performance](/guide/development/performance) for throughput/latency tuning

## Lambda-Focused Configuration

For AWS Lambda-targeted applications:

- Build-time platform override:
  - system property: `pipeline.platform=FUNCTION`
  - environment variable: `PIPELINE_PLATFORM=FUNCTION`
  - legacy aliases currently accepted for compatibility: `LAMBDA` and `STANDARD`
- Build-time transport override:
  - system property: `pipeline.transport=REST`
  - environment variable: `PIPELINE_TRANSPORT=REST`
- REST naming strategy:
  - system property: `pipeline.rest.naming.strategy=RESOURCEFUL|LEGACY`
  - environment variable: `PIPELINE_REST_NAMING_STRATEGY=RESOURCEFUL|LEGACY`

Operational keys commonly used with Lambda:

- `quarkus.snapstart.enabled`
- `JAVA_TOOL_OPTIONS=-XX:+TieredCompilation -XX:TieredStopAtLevel=1`

Function transport idempotency context attributes:

- `tpf.idempotency.policy=CONTEXT_STABLE|EXPLICIT` (legacy `RANDOM` is accepted as alias)
- `tpf.idempotency.key=<stable-caller-key>`

Examples of `tpf.idempotency.key` values:
- order ID (`order-12345`)
- transaction ID (`txn-8f2c1`)
- caller-provided idempotency token from an ingress API

Policy guidance:
- `CONTEXT_STABLE` (default): framework-generated deterministic transport keys from stable context/envelope identifiers.
- `EXPLICIT`: use when you can provide a stable business-side key that should remain constant across retries.
  `tpf.idempotency.key` is expected in this mode; if missing, adapters log a warning and fall back to `CONTEXT_STABLE`.
- `RANDOM`: legacy compatibility alias mapped to `CONTEXT_STABLE`.

Notes:
- Supplying `tpf.idempotency.key` while policy is `CONTEXT_STABLE`/`RANDOM` is ignored by key derivation.

These are transport-context attributes (ephemeral per invocation metadata propagated via `FunctionTransportContext`), not global runtime properties in `application.properties`.
They are usually set by handler/adapter code when creating `FunctionTransportContext`, for example:

```java
Map<String, String> attrs = Map.of(
    "tpf.idempotency.policy", "EXPLICIT",
    "tpf.idempotency.key", "order-12345");
FunctionTransportContext ctx = new FunctionTransportContext(requestId, functionName, "ingress", attrs);
```

TPF transport deduplication is best-effort; authoritative deduplication remains in business/data stores.

## Where to Read Next

- Lambda development model and gateway choices: [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- Lambda runtime operations and SnapStart: [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)
