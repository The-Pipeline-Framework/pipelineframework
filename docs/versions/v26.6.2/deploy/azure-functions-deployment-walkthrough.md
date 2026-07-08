---
search: false
---

# Azure Functions Deployment Walkthrough

This page keeps the deployment steps separate from the Search verification lane.

::: warning Preview Scope
The current path is Quarkus Azure Functions based. Spring/Azure function renderer parity is not available yet.
:::

## Prerequisites

- Java 21
- Maven 3.8+
- Azure Functions Core Tools v4.x for local runtime testing
- Azure CLI and Terraform for cloud deployment

## Local Runtime Shape

Build the Search orchestrator with the function platform settings:

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc -am \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dpipeline.rest.naming.strategy=RESOURCEFUL \
  -DskipTests package
```

Use Azure Functions Core Tools only after the generated function project has been prepared by the example scripts or Maven packaging path.

## Cloud Deployment Shape

The deployment walkthrough uses Terraform to provision Azure resources, then deploys the Quarkus Azure Functions package.

Keep credentials and resource names out of committed configuration. Store CI credentials in GitHub Secrets and keep local values in local-only shell or Terraform variable files.

For detailed command history and the original long-form notes, see [Search Azure Functions Reference](/versions/v26.6.2/deploy/search-azure-functions-reference#cloud-deployment-with-terraform).
