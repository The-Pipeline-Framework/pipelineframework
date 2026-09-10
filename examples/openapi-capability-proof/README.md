# OpenAPI Capability Proof

This application vendors an OpenAPI 3.1 contract, explicitly imports two `POST` operations as an
ordinary TPF Query and Command, and binds them into the existing packaged `callable-loop-proof`
Block. The contract uses path and query parameters, JSON bodies, a local external `$ref`, structured
response variants, OAuth/API-key declarations, an untrusted server template, and a discriminated
response adapted by a committed deterministic mapping.

The OpenAPI document is used only by the Maven importer in the `contracts` module. Runtime contains
immutable HTTP pins and compiler-generated representation bindings. The application owns the base
URI, borrowed HTTP client, already-resolved authorization material, tenant selection, and Command
authority. The Connector does not implement OAuth lifecycle.

The application invokes the operations both as local named Query/Command pipelines and through one
ordinary `pipeline: callable-loop-proof` step. The packaged loop performs Query → observation →
Command → observation → completion without OpenAPI-specific dispatch.

Refresh after intentionally changing the local contract or import selection:

```bash
./mvnw -f examples/openapi-capability-proof/contracts/pom.xml \
  openapi:refresh-import \
  -Dmaven.repo.local="$PWD/.m2/repository"
```

Normal builds run offline `verify-import` and fail if the committed provider manifest, pins, or
provenance are stale.

```bash
./mvnw -pl examples/openapi-capability-proof -am verify -Dmaven.repo.local="$PWD/.m2/repository"
```

The integration test verifies Query capture replay, Command duplicate replay, host-owned
authorization/base URI, explicit provider idempotency projection, sanitized release provenance,
and absence of the OpenAPI source from the runtime classpath.
