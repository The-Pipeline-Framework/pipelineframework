---
title: Block connector capabilities are application-bound
status: accepted
---

# ADR-0028: Block connector capabilities are application-bound

## Context

ADR-0024 established Blocks as statically imported version 3 pipeline definitions and initially
limited them to functional-core service and nested-pipeline steps. That restriction proved that
packaging and linking did not require a Block runtime, but it also prevented reusable composition
from containing ordinary Query and Command steps.

A reusable Block can own operation construction, validation, normalization, and topology without
owning the endpoint, credentials, tenant, authorization, or effect policy used by a consuming
application. Treating those application decisions as package configuration would transfer external
authority to the Block. Reimplementing Query or Command for Blocks would create a second execution
model and weaken capture, replay, idempotency, and ambiguity guarantees.

## Decision

A Block definition may declare named `QUERY` and `COMMAND` capability requirements in its schema 1
package manifest. Operation-first steps inside that definition use the requirement name in `using`.
The Block may select a provider operation and fixed operation configuration, but it cannot declare a
connector binding. Imported callables, dynamic operations, legacy Query/Command forms, inline
connectors, Await, checkpoint handoff, and remote/delegated execution remain forbidden.

The consuming application binds each requirement once per qualified Block definition through
compile-time `blockBindings`. The binding must refer to an application-owned connector binding. A
Command mapping must also explicitly select its command-ID generator, duplicate policy, and command
policy. The compiler validates the requirement kind and the provider manifest's exact operation
kind, major versions, cardinality, and canonical Java type contract before replacing the requirement
name with the application binding.

The compiler carries the resolved operation as immutable connector-selection IR. Generated
operation-first steps construct `QueryStepDescriptor.nativeQuery` or
`CommandDescriptor.nativeCommand` directly and invoke the existing `QueryStepSupport` or
`CommandStepSupport` path. Imported step and generated-class identities are derived from the
qualified Block definition and authored step name. There is no runtime Block lookup, registry, or
execution path.

Schema 3 `pipeline-contract.json` records both the package definition fingerprint and the linked
definition fingerprint. It also records sanitized requirement resolution: binding, provider and
operation versions, Command authority choices, and the existing connector-configuration digest.
These values participate in the contract hash. Raw connector configuration and credentials do not.

## Rationale

This divides ownership at the existing semantic boundary. A package can own reusable computation
and composition while the application retains every decision that grants external authority. Static
rewriting makes the result an ordinary normalized version 3 Query or Command, so existing capture,
cache, duplicate, retry, effect, and ambiguity behavior applies unchanged.

An explicit per-definition mapping avoids hidden global capability resolution and makes release
identity reproducible. Exact provider-manifest validation prevents a Block from being linked against
an operation that happens to have similar simple class names or an incompatible cardinality.

## Consequences

- Blocks may package ordinary operation-first Query and Command topology without packaging external
  bindings or credentials.
- Applications must consciously grant each imported definition the connector capabilities it uses;
  Command authority is never inherited from the package.
- `blockBindings` exists only during compilation and does not become runtime configuration.
- Changing package content, capability resolution, provider/operation versions, Command authority,
  or sanitized connector configuration changes the consuming release identity.
- Query capture/replay and Command effect/duplicate/ambiguity semantics remain owned by their
  existing runtime supports.
- Per-invocation Block binding, imported dynamic operations/callables, Await, packaged bindings,
  and runtime capability discovery remain future independent decisions.

This decision supersedes ADR-0024's initial functional-core-only import restriction. ADR-0024's
static-import, identity, type, composition, and no-Block-runtime decisions remain incorporated here.
