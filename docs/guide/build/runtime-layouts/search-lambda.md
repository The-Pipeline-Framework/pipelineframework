# Search Lambda Verification Lane

This page is a reference verification lane for `examples/search` in Function mode (AWS Lambda target).

Canonical Lambda development and operations guidance lives here:

- [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)

## Scope

- Verifies TPF Function platform wiring on a concrete example
- Verifies Quarkus Lambda mock event-server behavior locally
- Does not require provisioning a live AWS stack
- Uses current unary-only FUNCTION shape support (`UNARY_UNARY` only)
- For shape/bridge mapping and failure semantics, see [AWS Lambda Platform (Development)](/guide/development/aws-lambda)

## Build

```bash
./examples/search/build-lambda.sh -DskipTests
```

This sets:

- `pipeline.platform=FUNCTION`
- `pipeline.transport=REST`
- `pipeline.rest.naming.strategy=RESOURCEFUL`
- `pipeline.lambda.dependency.scope=compile`
- `quarkus.profile=lambda`

## Mock Event-Server Smoke Test

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  compile

./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

Expected result:

- `BUILD SUCCESS`
- `LambdaMockEventServerSmokeTest` passes
- pass/fail details are available in Maven Surefire summary and `orchestrator-svc/target/surefire-reports`

Operator note:

- Keep Lambda timeout and retry budget bounded for this lane; do not assume unbounded waits at function boundaries.

## Native Smoke Build

```bash
./mvnw -f examples/search/pom.xml \
  -pl crawl-source-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dpipeline.lambda.dependency.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  -Dquarkus.native.enabled=true \
  -Pnative \
  package
```
