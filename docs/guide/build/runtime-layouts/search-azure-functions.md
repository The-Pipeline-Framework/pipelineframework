# Search Azure Functions Testing Guide

This page is a reference testing guide for `examples/search` in Function mode (Azure Functions target).

Canonical Azure Functions development and operations guidance lives here:

- [Quarkus Azure Functions Extension](https://quarkus.io/guides/azure-functions)
- [Azure Functions Documentation](https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference)

## Architecture Notes

**Important**: The Pipeline Framework currently generates AWS Lambda-compatible function handlers (`RequestHandler<I, O>` with Lambda `Context`). For Azure Functions deployment, the framework uses the **REST transport** approach:

1. Azure Functions HTTP triggers route requests to Quarkus
2. Quarkus Azure Functions extension bootstraps the Quarkus runtime
3. Quarkus handles HTTP routing through its REST resources
4. The FUNCTION platform mode still applies for pipeline execution semantics

This means:
- **Local testing**: Use `func host start --java` with the helper script
- **Cloud deployment**: Use `quarkus:deploy` which configures the HTTP trigger
- **Pipeline behavior**: Same FUNCTION platform semantics (cardinality, failure handling)
- **Transport**: REST over HTTP triggers (not Lambda event payloads)

Future work may include native Azure Functions handler generation for direct function-to-pipeline binding.

## Scope

- Verifies TPF (The Pipeline Framework) Function platform wiring on Azure Functions
- Verifies Quarkus Azure Functions extension behavior locally and in cloud
- Verifies non-unary FUNCTION bridge paths through targeted runtime/deployment tests
- Supports both local testing (Azure Functions Core Tools) and cloud deployment (Terraform-provisioned resources)
- Uses Search pipeline fan-out/fan-in cardinalities (`ONE_TO_MANY`, `MANY_TO_ONE`)
- Keep Azure Functions timeout and retry budget bounded for this verification lane; do not assume unbounded waits at function boundaries.

## Prerequisites

### Local Development

- **Java 21**
- **Maven 3.8+**
- **Azure Functions Core Tools v4.x** (for local testing)
- **Azure CLI** (for authentication and deployment)

### Cloud Deployment (CI/CD)

- Azure subscription with permissions to create:
  - Resource Groups
  - Function Apps (Premium plan)
  - Storage Accounts
  - Application Insights
  - Service Plans
- GitHub Actions secrets configured:
  - `AZURE_CLIENT_ID`
  - `AZURE_TENANT_ID`
  - `AZURE_SUBSCRIPTION_ID`

## Build

### Local Build with Azure Functions Profile

```bash
./examples/search/build-azure.sh -DskipTests
```

This sets:

- `tpf.build.platform=FUNCTION`
- `tpf.build.transport=REST`
- `tpf.build.rest.naming.strategy=RESOURCEFUL`
- `tpf.build.azure.scope=compile`
- `quarkus.profile=azure-functions`

### Manual Build with Maven

```bash
./mvnw -f examples/search/pom.xml \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -DskipTests \
  clean install
```

## Local Testing with Azure Functions Core Tools

### Bootstrap Smoke Test

Verify that Azure Functions extension compiles and loads correctly:

```bash
./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -Dtest=AzureFunctionsBootstrapSmokeTest \
  test
```

Expected result:

- `BUILD SUCCESS`
- `AzureFunctionsBootstrapSmokeTest` passes

### Local Function Runtime Testing

**Important**: Quarkus dev mode and `quarkus:run` do not work with Azure Functions (see [Quarkus Azure Functions Guide](https://quarkus.io/guides/azure-functions#development-mode)). The extension generates the required function project structure (`host.json`, `function.json`) only during `quarkus:deploy`.

For local runtime testing with Azure Functions Core Tools, use the helper script to prepare the project structure:

```bash
cd examples/search

# Build the package
./mvnw clean package \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -DskipTests

# Prepare Azure Functions project structure (creates host.json, local.settings.json)
./prepare-azure-functions-local.sh

# Run with Azure Functions Core Tools (from examples/search directory where host.json lives)
func host start --java
```

The function will be available at:
- HTTP Trigger URL: `http://localhost:7071/api/{route}`
- Health endpoint: `http://localhost:7071/q/health` (if configured)

**Prerequisites**: Azure Functions Core Tools v4.x must be installed:

```bash
# macOS
brew tap azure/functions
brew install azure-functions-core-tools@4

# Windows (Chocolatey)
choco install azure-functions-core-tools

# Linux (Debian/Ubuntu)
curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
sudo mv microsoft.gpg /etc/apt/trusted.gpg.d/microsoft.gpg
sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/debian/$(lsb_release -rs | cut -d'.' -f 1)/prod $(lsb_release -cs) main" > /etc/apt/sources.list.d/dotnetdev.list'
sudo apt-get update && sudo apt-get install azure-functions-core-tools-4
```

**Note**: The helper script creates minimal `host.json` and `function.json` files for local testing. For production deployment, use `quarkus:deploy` which generates the complete function structure.

## Cloud Deployment with Terraform

### Terraform Infrastructure Provisioning

The Search pipeline uses Terraform to provision Azure Functions infrastructure for E2E testing:

```bash
cd examples/search/terraform

# Initialize Terraform
terraform init

# Plan deployment
terraform plan \
  -var="resource_group_name=rg-search-pipeline-test" \
  -var="function_app_name=funcsearchtest" \
  -var="storage_account_name=stsearchtest" \
  -var="location=westus"

# Apply deployment
terraform apply
```

### Deploy to Azure Functions

After Terraform provisions infrastructure:

```bash
cd examples/search/orchestrator-svc

# Deploy using Quarkus Maven plugin
./mvnw quarkus:deploy \
  -Dtpf.build.platform=FUNCTION \
  -Dtpf.build.transport=REST \
  -Dtpf.build.rest.naming.strategy=RESOURCEFUL \
  -Dtpf.build.azure.scope=compile \
  -Dquarkus.profile=azure-functions \
  -Dquarkus.azure-functions.app-name=funcsearchtest \
  -Dquarkus.azure-functions.region=westus
```

### Run E2E Tests Against Deployed Function

```bash
export AZURE_FUNCTION_APP_URL="https://funcsearchtest.azurewebsites.net"

./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -DskipUnitTests=true \
  -Dit.test=AzureFunctionsEndToEndIT \
  -Dazure.function.app.url="$AZURE_FUNCTION_APP_URL" \
  verify
```

### Cleanup Resources

```bash
cd examples/search/terraform

# Destroy all provisioned resources
terraform destroy \
  -var="resource_group_name=rg-search-pipeline-test" \
  -var="function_app_name=funcsearchtest" \
  -var="storage_account_name=stsearchtest"
```

## GitHub Actions CI/CD

The Search Azure Functions E2E test runs automatically in CI when triggered:

```yaml
# .github/workflows/e2e-search-azure-functions.yml
name: Reusable — Search Azure Functions E2E

on:
  workflow_call:
    inputs:
      artifact_name:
        description: 'Name of the Maven artifacts tarball'
        required: false
        default: 'framework-m2-cache'
        type: string
```

### CI Workflow Steps

1. **Azure Login** (OIDC authentication)
2. **Terraform Init/Plan/Apply** (provision infrastructure)
3. **Build Function Artifacts** (Maven package)
4. **Deploy to Azure Functions** (Quarkus deploy)
5. **Run E2E Tests** (AzureFunctionsEndToEndIT)
6. **Terraform Destroy** (cleanup resources)

### Required GitHub Secrets

Configure these secrets in your repository or organization:

- `AZURE_CLIENT_ID`: Service principal or federated identity client ID
- `AZURE_TENANT_ID`: Azure AD tenant ID
- `AZURE_SUBSCRIPTION_ID`: Azure subscription ID

## Non-Unary Function Bridge Lane

Run targeted tests that validate generated non-unary function bridge paths for Search shape mappings:

```bash
# Framework runtime tests
./mvnw -f framework/pom.xml \
  -pl runtime \
  -Dtest=FunctionTransportBridgeTest,FunctionTransportAdaptersTest \
  test

# Framework deployment tests
./mvnw -f framework/pom.xml \
  -pl deployment \
  -Dtest=RestFunctionHandlerRendererTest,OrchestratorFunctionHandlerRendererTest \
  test
```

These tests validate:

- `ONE_TO_MANY` (`UNARY_STREAMING`) -> `FunctionTransportBridge.invokeOneToMany(...)`
- `MANY_TO_ONE` (`STREAMING_UNARY`) -> `FunctionTransportBridge.invokeManyToOne(...)`
- Generated handler wiring for non-unary FUNCTION shapes

## Test Coverage

### AzureFunctionsBootstrapSmokeTest

- Verifies Azure Functions extension classes are loadable
- Basic compilation and classpath validation
- Runs locally without Azure credentials

### AzureFunctionsEndToEndIT

End-to-end tests against deployed Azure Functions:

- **testFunctionAppHealthEndpoint**: Basic connectivity check
- **testPipelineRunUnaryInvocation**: ONE_TO_ONE pipeline execution
- **testFanOutFanInCardinality**: ONE_TO_MANY → MANY_TO_ONE execution
- **testMultipleSequentialInvocations**: Statelessness verification

### Test Data

Tests use dynamically generated test data:

- Unique `docId` per test invocation
- Deterministic pipeline execution through Function boundaries
- Response validation for cardinality aggregation

## Azure Functions vs AWS Lambda

| Aspect | AWS Lambda | Azure Functions |
|--------|-----------|-----------------|
| Extension | `quarkus-amazon-lambda` | `quarkus-azure-functions` |
| Local Testing | Mock event server | Core Tools v4.x runtime |
| Deployment | SAM/CloudFormation | Quarkus deploy / Azure CLI |
| Trigger Model | Event-based | HTTP trigger + bindings |
| Identity | IAM roles | Managed identity |
| Monitoring | CloudWatch | Application Insights |
| Cold Start | SnapStart | Premium plan |

## Troubleshooting

### Common Issues

**Function deployment fails:**
```
ERROR: Function app name already exists globally
```
Solution: Use a unique `function_app_name` in Terraform variables.

**Local testing fails with Core Tools error:**
```
Azure Functions Core Tools not found
```
Solution: Install Core Tools v4.x:
```bash
# macOS
brew tap azure/functions
brew install azure-functions-core-tools@4

# Windows (Chocolatey)
choco install azure-functions-core-tools
```

**Authentication errors during deployment:**
```
ERROR: Please run az login to setup credentials
```
Solution: Authenticate with Azure CLI:
```bash
az login --scope https://management.core.windows.net//.default
```

**Function timeout during E2E tests:**
```
java.net.http.HttpTimeoutException: request timed out
```
Solution: Increase timeout in test or check Function App logs in Azure Portal.

### Logging and Monitoring

Access Function logs via:

1. **Azure Portal**: Function App → Monitor → Logs
2. **Application Insights**: Query with Kusto
3. **CLI**: `az functionapp log tail`

```bash
az functionapp log tail \
  --name funcsearchtest \
  --resource-group rg-search-pipeline-test
```

## Performance Considerations

### Cold Start Mitigation

- Use **Premium plan** (`EP1` or higher) for production workloads
- Enable **always_on** in site configuration
- Consider **custom warm-up endpoint** for critical paths

### Scaling

- Premium plan supports **pre-warmed instances**
- Configure **scale-out rules** based on HTTP queue length
- Monitor **function execution time** and **memory pressure**

## Next Steps

- Review [AWS Lambda Platform (Development)](/guide/development/aws-lambda) for Function platform concepts
- See [Search Lambda Verification Lane](search-lambda.md) for AWS Lambda-specific guidance
- Consult [Quarkus Azure Functions Guide](https://quarkus.io/guides/azure-functions) for extension details
