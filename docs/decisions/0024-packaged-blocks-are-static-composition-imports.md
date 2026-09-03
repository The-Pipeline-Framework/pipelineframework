---
title: Packaged blocks are static composition imports
status: accepted
---

# ADR-0024: Packaged blocks are static composition imports

## Context

Applications can already compose named version 3 pipeline definitions as typed steps. Reusable
deterministic computation, however, previously had to remain application-local or be exposed through
application-owned libraries and adapter services. That left no distribution mechanism corresponding
to ordinary functional-core behavior: Aspects own cross-cutting effects, Operators own reused local
or remote execution references, Connectors own external I/O, and contributed types own protocol
vocabulary, but none of those abstractions owns a packaged typed transformation.

A distributable computation must preserve the existing composition, cardinality, representation,
validation, and release-identity authorities. Introducing a block executor, runtime registry, or
package-specific classloader would create a second execution model for behavior that is already an
ordinary named pipeline after linking.

## Decision

A block artifact publishes compiler-visible identity, artifact provenance, and one or more version
3 definition resources. The compiler discovers those resources statically from application
dependencies, assigns every imported definition the stable identity `namespace/logical-name`, merges
its package-owned canonical types into the application's normalized version 3 type model, and links
the result through the existing named-pipeline resolver and linker.

An unqualified application `pipeline:` reference resolves to an imported definition only when the logical
name is unique and does not collide with a local definition. Within one block package, an internal short
reference resolves to that package's own definition even when another package exports the same short name;
cross-package references can use the qualified identity directly. Duplicate qualified identities,
local/imported collisions, ambiguous application short names, and canonical type collisions fail compilation.

A type contributed by a block may bind its canonical identity to a package-owned Java class. That
binding remains part of the ordinary compiler-owned type model; it does not create a parallel package
type system. Existing representation providers may materialize a package-owned boundary exactly as
they materialize an application-owned boundary.

The initial import validator accepts only authored service transformations and nested `pipeline:`
composition. Query, Command, Await, remote Operator/delegate declarations, connector authority, and
checkpoint behavior are rejected inside imported definitions. Their independent packaging semantics
require separate decisions.

Generated schema version 3 release metadata records the qualified definition, namespace, artifact
coordinates and version, resource path, and normalized definition fingerprint. Those values
participate in the existing contract hash. This is an intentional breaking extension of schema
version 3 rather than a new schema version.

## Rationale

Static normalization makes an imported definition indistinguishable from equivalent local
composition before runtime generation begins. The existing linker remains the source of truth for
contract compatibility, cardinality, cycles, invocation locations, and normalized topology. The
existing runtime continues to execute only generated local steps and nested pipeline invocations.

Package-owned Java bindings let an artifact carry both vocabulary and implementation without forcing
the application to recreate boundary records or conversion adapters. Artifact coordinates plus a
definition fingerprint make imported behavior inspectable and ensure a semantic package change alters
the consuming release identity.

## Consequences

- Installing a block is a build dependency operation; there is no runtime download, registry, or
  discovery path.
- Block implementation classes and transitive libraries use the application's normal classpath and
  dependency injection mechanisms.
- Imported and local definitions share one composition graph, one generated contract, and one runtime
  invocation mechanism. No parallel block graph is generated.
- Package definitions cannot silently shadow local definitions or one another.
- Package-owned canonical Java types are available to ordinary application steps, mappers, and input
  boundaries without application-owned copies.
- Packaging external authority, generalized block parameters, and dynamic runtime loading remain
  out of scope.
