---
title: Release packaging facts share the immutable descriptor
status: draft
---

# ADR-0088: Release Packaging Facts Share the Immutable Descriptor

## Context

ADR-0064 defines a closed release over final artefact bytes and compiler-owned
semantics. A Java Lambda package can contain several framework entry classes,
while two separate packages can embed the same application. Consumers need
explicit physical entry associations to validate those packages independently.

## Decision

An optional schema-2 deployment section carries packaging facts in the existing
immutable Release Descriptor. It names a versioned provider profile, package
entries and logical binding requirements. An entry identifies an existing
artefact, its compiled module, the exact application and handler package
locations, Java entry class and trigger. A bounded declared check identifies
its input and expected result.

The shared contracts validate entries and compare each selected package's
complete Compiled Truth with the release carrier. Runtime's shared producer and
Maven adapter emit the same model; the native CLI verifies its closure.
The declared carrier location governs extraction. For Lambda ZIPs, a handler
association must also agree with classpath precedence; a shadowed copy in the
declared JAR does not establish the association.
Schema-1 constructors and JSON remain supported, and remain the default when
deployment facts are absent.

The compiler owns Command, Query and connector requirements. Runtime's build
integration records the selected durable store implementations. Admission
compares these facts with explicit environment grants before admitting effects.

Accounts, tenant identities, resource identifiers, credentials, secret
generations, endpoint exposure and grants belong to Cloud's deployment binding.
A provider profile and a declared check require provider-side qualification.
Producing or verifying a release does not execute that check.

## Rationale

Physical packaging facts belong with the bytes they describe. Keeping one
descriptor avoids an independent topology authority, while compiler metadata
remains the authority for application semantics.

## Consequences

Multiple framework entries can share a package without duplicating ownership of
authored steps. Package verification rejects a foreign compiled application,
missing handler, incomplete Compiled Truth and missing durable-store facts.
Supported Java archive forms are validated before use. Schema-2 entries for
unsupported package forms fail rather than inheriting unproven support.

Application authors keep framework release production and host adaptation in
TPF tooling. Cloud separately qualifies physical delivery, retained versions,
tenant authority and live readiness.
