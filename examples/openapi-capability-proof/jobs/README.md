# OpenAPI Command Callback Proof

This application contains one native `job.start` Command with `await.callback: job.completed`.
`StartJobRequest` contains only `jobId` and `value`. TPF injects the host-resolved callback URI into
the provider's required `callbackUrl` field after the direct request mapping. The provider returns
`JobAccepted`; an authenticated `JobCallback` is projected with the original request into `JobResult`.

The shared contracts module pins the initiating operation, external completion schema, JSON media
type, callback signature requirement, and 202 acknowledgement. Its source server template is
descriptive only and differs from the fake provider used at runtime.

Application Java owns the Command id, host connection, public callback base, raw-body HMAC policy,
and pure completion projector. TPF owns callback routing, durable correlation, admission,
duplicate handling, dispatch settlement, timeout, and continuation. There is no application router,
Await store, preparation step, or continuation service.

Run from the repository root with Docker available:

```bash
./mvnw -pl examples/openapi-capability-proof/jobs -am verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

`JobCallbackProofIT` exercises the generated flow and real callback ingress. It covers early
completion held until HTTP 202, ambiguous dispatch completed by callback, bounded invalid-request
rejection, duplicate acknowledgement, timeout, and one control-plane-admitted
`RETRY_FAILED_COMMAND` after proven non-dispatch. The retry preserves Command, interaction,
callback URI, and provider idempotency identities. Ordinary replay never redispatches a retained
failed effect.

`PackagedJobCallbackIT` starts the actual JAR from an empty working directory and uses the existing
hosted execution and worker administration APIs. It persists a wait in TPF's DynamoDB stores,
stops and restarts the JVM, delivers the original signed callback, and verifies the final value,
unchanged Command evidence, one provider effect, and duplicate acknowledgement. It inspects every
runtime JAR to exclude the source OpenAPI YAML and Swagger Parser. Process logs are retained under
`target/packaged-callback-*.log`.

The packaged application selects Dynamo execution and Command effect stores. The integration
harness provisions their standard table schemas and supplies connection and signing configuration.
The in-process lifecycle tests explicitly select memory providers. Provider selection is part of
the application build; changing the Command effect provider requires rebuilding.

Host configuration supplies `openapi.proof.jobs-base-uri`, `openapi.proof.callback-base-uri`,
`openapi.proof.callback-signing-key`, and `pipeline.orchestrator.resume-token-secret`. The fixture
uses local HTTP only with the explicit `pipeline.callback.allow-http=true` policy. HTTPS is the
default for both provider and public callback addresses. Callback URLs and signing material must
stay out of authored input, release pins, and logs.

See [Command runtime setup](../../../docs/deploy/orchestrator-runtime/command.md),
[OpenAPI import](../../../docs/develop/connectors/openapi-import.md), and
[Await operations](../../../docs/operate/await-boundaries.md).
