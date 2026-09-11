# OpenAPI Capability Proof

This example vendors an OpenAPI 3.1 contract and explicitly imports three `POST` operations. The
original application uses an ordinary Query and synchronous Command directly and through the
existing packaged `callable-loop-proof` Block. The sibling [`jobs`](./jobs) application runs one
native Command with `await.callback`. The contract uses path and query parameters, JSON bodies, a local external `$ref`, structured
response variants, OAuth/API-key declarations, an untrusted server template, and a discriminated
response adapted by a committed deterministic mapping.

The OpenAPI document is used only by the Maven importer in the `contracts` module. Runtime contains
immutable HTTP pins and compiler-generated representation bindings. The application owns the base
URI, borrowed HTTP client, already-resolved authorization material, tenant selection, and Command
authority. The Connector does not implement OAuth lifecycle.

The application invokes the operations both as local named Query/Command pipelines and through one
ordinary `pipeline: callable-loop-proof` step. The packaged loop performs Query → observation →
Command → observation → completion without OpenAPI-specific dispatch.

After intentionally changing the local contract, review and update its `source.closureSha256` pin.
Refresh the committed import outputs after changing the contract or import selection:

```bash
./mvnw -f examples/openapi-capability-proof/contracts/pom.xml \
  openapi:refresh-import \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

Normal builds run offline `verify-import` and fail if the committed provider manifest, pins, or
provenance are stale.

```bash
./mvnw -pl examples/openapi-capability-proof,examples/openapi-capability-proof/jobs -am verify \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

The integration test verifies Query capture replay, Command duplicate replay, host-owned
authorization/base URI, explicit provider idempotency projection, sanitized release provenance,
and absence of the OpenAPI source from the runtime classpath.

The jobs proof adds early and late signed callbacks, ambiguous dispatch, Await timeout, deliberate
pre-send retry with stable identities, and a real packaged JVM restart backed by DynamoDB. Its
packaged test starts LocalStack and the fake provider automatically; Docker must be available.
