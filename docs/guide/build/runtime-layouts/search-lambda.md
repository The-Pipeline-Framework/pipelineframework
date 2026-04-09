# Search Modular AWS Lambda Guide

This page describes the supported AWS Lambda deployment path for `examples/search`.

Canonical Lambda development and operations guidance lives here:

- [AWS Lambda Platform (Development)](/guide/development/aws-lambda)
- [AWS Lambda SnapStart (Operate)](/guide/operations/aws-lambda-snapstart)

## Scope

- Treats Search as a modular runtime layout on AWS Lambda
- Deploys 5 Lambdas:
  - `orchestrator-svc`
  - `crawl-source-svc`
  - `parse-document-svc`
  - `tokenize-content-svc`
  - `index-document-svc`
- Uses a simplified Search pipeline definition without persistence or cache aspects
- Uses the Quarkus AWS HTTP bridge behind Lambda Function URLs
- Reuses the orchestrator REST client override contract via `QUARKUS_REST_CLIENT_*_URL`
- Does not provision or require Postgres or Redis
- Is intended for manual testing with disposable infrastructure and is not part of the default CI workflows

## Runtime Files

The modular AWS deployment path separates runtime placement from the pipeline definition:

- Runtime placement: `examples/search/config/pipeline.runtime.yaml`
- AWS-specific aspect-free pipeline definition: `examples/search/config/pipeline.modular-lambda.yaml`

`pipeline.runtime.yaml` remains the canonical runtime-mapping filename. The dedicated AWS config is a separate pipeline definition and does not replace it.

## Build The 5 Lambda Artifacts

```bash
./examples/search/build-lambda-modular.sh -DskipTests -Dquarkus.container-image.build=false
```

This sets:

- `tpf.build.platform=FUNCTION`
- `tpf.build.transport=REST`
- `tpf.build.rest.naming.strategy=RESOURCEFUL`
- `tpf.build.lambda.scope=test`
- `tpf.build.lambda.http.scope=compile`
- `quarkus.profile=lambda-modular`
- `-Apipeline.config=examples/search/config/pipeline.modular-lambda.yaml`

After the build, each deployable module emits a `target/function.zip`.

## Manual AWS Deployment

Terraform for the modular lane lives under `examples/search/terraform/aws-modular`.

```bash
cd examples/search/terraform/aws-modular

terraform init
terraform apply \
  -var="aws_region=us-east-1" \
  -var="name_prefix=search-modular-$(date +%s)"
```

Outputs include the orchestrator Function URL and all downstream step Function URLs.

## Run The Modular AWS E2E Test

```bash
export AWS_LAMBDA_ORCHESTRATOR_URL="$(terraform -chdir=examples/search/terraform/aws-modular output -raw orchestrator_function_url)"

./mvnw -f examples/search/pom.xml \
  -pl orchestrator-svc \
  -am \
  -DskipUnitTests=true \
  -Dfailsafe.failIfNoSpecifiedTests=false \
  -Dquarkus.container-image.build=false \
  -Dit.test=AwsLambdaModularEndToEndIT \
  verify
```

The test invokes only the orchestrator URL. Downstream routing is driven by the deployed `QUARKUS_REST_CLIENT_PROCESS_*_URL` values.

## Destroy The AWS Resources

```bash
terraform -chdir=examples/search/terraform/aws-modular destroy \
  -var="aws_region=us-east-1" \
  -var="name_prefix=<the-same-prefix>"
```

## GitHub Actions

Manual GitHub validation lives in `.github/workflows/e2e-search-aws-modular.yml`.

Required repository settings:

- secret: `AWS_ROLE_ARN`
- variable: `AWS_REGION`

This workflow uses GitHub OIDC and must be triggered manually with `workflow_dispatch`.

## Historical Single-Lambda Smoke Path

The older `./examples/search/build-lambda.sh` path remains useful as a local wiring smoke test for the orchestrator module and the generated direct-handler path.

If you use that direct `%lambda` path outside the local smoke test, the client truststore password
can be overridden with `CLIENT_TRUSTSTORE_PASSWORD`; it keeps the `secret` default for the packaged
dev certificate path.

It is not the supported live AWS deployment topology for Search. The supported live AWS lane is the modular 5-Lambda topology described above.
