# AWS Lambda SnapStart (Operate)

This page covers operational guidance for running TPF applications on AWS Lambda with SnapStart.

## Quarkus SnapStart Support

With Quarkus Lambda applications, SnapStart support is included by the Quarkus Lambda stack and enabled by default.

Relevant keys:

- `quarkus.snapstart.enabled` (current)
- `quarkus.snapstart.enable` (deprecated alias)

Disable only when you explicitly need to:

```properties
quarkus.snapstart.enabled=false
```

## JVM Startup Tuning

For SnapStart, Quarkus recommends tiered compilation configuration via environment variable:

```bash
JAVA_TOOL_OPTIONS="-XX:+TieredCompilation -XX:TieredStopAtLevel=1"
```

This setting can also be useful for regular (non-SnapStart) Lambda functions with cold-start sensitivity.

## Recommended Operational Checks

1. Verify Lambda runtime, memory, and timeout match your pipeline latency profile.
2. Track cold-start and init duration separately from request duration.
3. Validate tracing/metrics exporters under constrained Lambda memory sizes.
4. Keep native and JVM deployment lanes in CI for regression coverage.

## Tracing on Lambda

For AWS X-Ray integration on Quarkus Lambda:

- add `io.quarkus:quarkus-amazon-lambda-xray`

Then configure tracing/export settings according to your observability topology.

## Related Docs

- [Tracing](/guide/operations/observability/tracing)
- [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- [Configuration (Overview)](/guide/application/configuration)
