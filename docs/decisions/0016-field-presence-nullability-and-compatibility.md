---
title: Field presence, nullability, and compatibility are independent
status: accepted
---

# ADR-0016: Field presence, nullability, and compatibility are independent

## Context

A singular Java reference can be null, proto3 can retain field presence, and canonical JSON
can distinguish a missing key from an explicit null. Treating those facts as one `optional`
flag makes compatibility claims depend on whichever representation happens to be inspected.

## Decision

Every singular v3 record field has independent `presence` and `nullability` semantics.
Unmarked fields are `REQUIRED` and `NON_NULL`. Authored punctuation is parsed away before
normalization; normalized IDL and hashed contract metadata carry the explicit enum values.
Defaults remain a separate, future semantic axis.

Compatibility compares the previous normalized IDL with the current normalized IDL and reports
consequences separately for normalized semantics, protobuf wire data, canonical data, and generated
Java source/domain APIs. Protobuf presence never determines domain requiredness.

Nullable protobuf fields use a compiler-owned `oneof` value/null-marker encoding so absent,
explicit null, and a concrete value remain distinct. Non-default generated Java fields use an
explicit three-state carrier rather than nullable references. Repeated fields retain their existing
finite-list semantics and do not yet accept presence or nullability modifiers.

## Rationale

One normalized semantic owner lets JSON validation, protobuf generation, Java generation,
contract hashing, and evolution diagnostics agree without collapsing distinctions at a convenient
target boundary.

## Consequences

- Adding a required field is protobuf-wire compatible but canonical-data incompatible.
- Widening and narrowing changes can have different outcomes on different surfaces.
- Nullable protobuf fields consume and persist an additional compiler-owned tag and name.
- Removing a nullable field, or making it non-null, reserves its former null-marker identity.
- Existing unmarked v3 fields keep the strongest required/non-null behavior.
