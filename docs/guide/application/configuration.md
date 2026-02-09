# Configuration (Overview)

This page maps the most-used configuration entry points and where to manage them for TPF applications.

## Primary References

- [Configuration Reference](/guide/build/configuration/) for full build-time and runtime key catalog
- [Performance](/guide/development/performance) for throughput/latency tuning

## Lambda-Focused Configuration

For AWS Lambda-targeted applications:

- Build-time platform override:
  - system property: `pipeline.platform=LAMBDA`
  - environment variable: `PIPELINE_PLATFORM=LAMBDA`
- Build-time transport override:
  - system property: `pipeline.transport=REST`
  - environment variable: `PIPELINE_TRANSPORT=REST`
- REST naming strategy:
  - system property: `pipeline.rest.naming.strategy=RESOURCEFUL|LEGACY`
  - environment variable: `PIPELINE_REST_NAMING_STRATEGY=RESOURCEFUL|LEGACY`

Operational keys commonly used with Lambda:

- `quarkus.snapstart.enabled`
- `JAVA_TOOL_OPTIONS=-XX:+TieredCompilation -XX:TieredStopAtLevel=1`

## Where to Read Next

- Lambda development model and gateway choices: [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- Lambda runtime operations and SnapStart: [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)
