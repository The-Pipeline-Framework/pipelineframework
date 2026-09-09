# GraphQL Block proof

This Quarkus application proves that the production `graphql-agent` Block can package a bounded,
GraphQL-aware callable loop while the application retains external authority.

The Block owns turn preparation, model tools, trusted effect-key derivation, Query/Mutation routing,
observation normalization, reduction, bounded recursion, and typed completion. The application owns
the LLM and `graphql.smallrye` bindings, digest-pinned Query and Mutation documents, host
`ConnectionResolver`, effect scope, and explicit Command identity/duplicate/policy choices. Its root
pipeline contains only `pipeline: graphql-agent`.

The deterministic integration proof performs persisted Query → partial-error Mutation → typed
completion. It verifies LLM and GraphQL Query capture replay, Command duplicate replay without live
redispatch, model-input exclusion of effect authority, trusted Mutation argument injection, turn
exhaustion before another external call, and sanitized generated provenance. The consumer contains
no GraphQL routing, reducer, recursion, transport, or normalization implementation.
