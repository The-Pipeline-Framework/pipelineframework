# Search Azure Functions Verification Lane

This page is the bounded verification lane for `examples/search` in Function mode on Azure Functions.

::: warning Preview Verification Lane
Azure Functions support for `examples/search` is preview, manual, and optional for CI. Treat this page as a verification path, not as a general production deployment recipe.
:::

## What This Lane Proves

- TPF `FUNCTION` platform wiring can target Azure Functions for the Search example.
- Generated Azure handlers can bootstrap through the Quarkus Azure Functions extension.
- Search fan-out/fan-in cardinalities are exercised against the function bridge.
- Provider-specific behavior remains separate from core pipeline contracts.

## Run The Local Build

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc -am \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -DskipTests compile
```

## Run The Smoke Test

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -Dpipeline.platform=FUNCTION \
  -Dpipeline.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -Dtest=AzureFunctionsBootstrapSmokeTest test
```

## Related Pages

- [Azure Functions Platform](/deploy/azure-functions)
- [Azure Functions Deployment Walkthrough](/deploy/azure-functions-deployment-walkthrough)
- [Azure Functions Troubleshooting](/deploy/azure-functions-troubleshooting)
- [Search Azure Functions Reference](/deploy/search-azure-functions-reference)
