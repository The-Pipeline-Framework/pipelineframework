---
title: Hibernate find.many providers preserve streaming and declared total order
status: accepted
---

# ADR-0022: Hibernate `find.many` providers preserve streaming and declared total order

## Context

ADR-0019 makes a finite streaming Query an ordinary TPF `ONE_TO_MANY` expansion. Hibernate ORM
offers a blocking result stream, while Hibernate Reactive exposes list-oriented query results rather
than a row publisher. Treating either API as `List<Row>` pipeline semantics would discard demand,
cancellation, and bounded resource ownership. Retried source expansions also derive stable child
identity from output ordinal, so provider ordering must be total and repeatable.

## Decision

Both first-party Hibernate providers expose `find.many` version 1 through
`StreamingQueryOperation`. They share the same predicate, projection, ordering, and semantic-limit
configuration. `orderBy` is required and `uniqueBy` declares the non-empty unique suffix of that
ordered path list. Providers reject adjacent duplicate complete ordering tuples while reading; a
database observation that violates the declared total order fails instead of silently assigning
unstable child lineage.

The blocking `jpa.query` provider uses JPA `getResultStream()` behind framework-owned blocking
iterator support. Cursor opening, row reads, projection, and closure run on framework workers. It
does not materialize the complete observation or own an executor.

The non-blocking `hibernate.reactive.query` provider owns one Hibernate Reactive session for the
observation. Its private demand adapter permits one query in flight and fetches at most the smaller
of outstanding row demand, 64 rows, and the remaining semantic limit. Offset windows and bounded
`getResultList()` calls are provider I/O mechanics; they are not pages or items in the pipeline
contract. Mutiny and Hibernate Reactive types remain provider-private.

In both providers, cancellation stops further reads and resource termination completes only after
the stream, entity manager, or reactive session has closed. A semantic `limit` bounds emitted rows;
it does not make an otherwise unbounded materialization acceptable.

## Rationale

The shared provider contract describes row streaming, while each Hibernate family uses the lowest
sensible native API. Requiring an explicit unique ordering suffix makes the retry/lineage condition
visible and bindable. Runtime duplicate detection catches false uniqueness declarations without
retaining the full result set.

## Consequences

- `find.one` remains unary and unchanged; `find.many` has no unary `result` field.
- Generic Query cache remains unavailable for streaming operations. Query capture stages ordered
  rows and commits only a successful terminal observation.
- The reactive provider may re-run ordered offset queries as demand arrives. Snapshot isolation
  across concurrent database mutation and source checkpoint/resume remain out of scope.
- Finite streams may still be too large to collect. Any transport or caller that materializes them
  must enforce its own explicit resource bound.
