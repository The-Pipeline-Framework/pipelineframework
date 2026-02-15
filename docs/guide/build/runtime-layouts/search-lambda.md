# Search Lambda Verification Lane

This page is a reference verification lane for `examples/search` in Function mode (AWS Lambda target).

Canonical Lambda development and operations guidance lives here:

- [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)

## Scope

- Verifies TPF Function platform wiring on a concrete example
- Verifies Quarkus Lambda mock event-server behavior locally
- Verifies non-unary FUNCTION bridge paths through targeted runtime/deployment tests
- Does not require provisioning a live AWS stack
- Uses Search pipeline fan-out/fan-in cardinalities (`ONE_TO_MANY`, `MANY_TO_ONE`)
- For shape/bridge mapping and failure semantics, see [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- Keep Lambda timeout and retry budget bounded for this verification lane; do not assume unbounded waits at function boundaries.

## Build

```bash
./examples/search/build-lambda.sh -DskipTests
```

This sets:

- `tpf.build.platform=FUNCTION`
- `tpf.build.transport=REST`
- `tpf.build.rest.naming.strategy=RESOURCEFUL`
- `tpf.build.lambda.scope=compile`
- `quarkus.profile=lambda`

## Mock Event-Server Smoke Test

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc -am \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  compile

./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -Dtest=LambdaMockEventServerSmokeTest \
  test
```

Expected result:

- `BUILD SUCCESS`
- `LambdaMockEventServerSmokeTest` passes
- pass/fail details are available in Maven Surefire summary and `orchestrator-svc/target/surefire-reports`

## Non-Unary Function Bridge Lane

Run targeted tests that execute generated non-unary function bridge paths for Search shape mappings:

```bash
./mvnw -f framework/pom.xml \
  -pl runtime \
  -Dtest=FunctionTransportBridgeTest,FunctionTransportAdaptersTest \
  test

./mvnw -f framework/pom.xml \
  -pl deployment \
  -Dtest=RestFunctionHandlerRendererTest,OrchestratorFunctionHandlerRendererTest \
  test
```

These tests validate:

- `ONE_TO_MANY` (`UNARY_STREAMING`) -> `FunctionTransportBridge.invokeOneToMany(...)`
- `MANY_TO_ONE` (`STREAMING_UNARY`) -> `FunctionTransportBridge.invokeManyToOne(...)`
- generated handler wiring for non-unary FUNCTION shapes

## Native Smoke Build

```bash
./mvnw -f examples/search/pom.xml \
  -pl crawl-source-svc -am \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.lambda.scope=compile \
  -Dquarkus.profile=lambda \
  -DskipTests \
  -Dquarkus.native.enabled=true \
  -Pnative \
  package
```
