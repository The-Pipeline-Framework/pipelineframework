# GraphQL Block proof

This Quarkus application proves that an installed Block can contribute ordinary GraphQL Query and
Command computation while the application retains external authority.

The application owns the `graphql.smallrye` binding, digest-pinned Query and Mutation documents,
host `ConnectionResolver`, mutation effect key, and `blockBindings` Command policy. It invokes only
the imported `graphql-query` and `graphql-mutation` definitions through ordinary `pipeline:` steps.
It contains no GraphQL parsing, request transport, routing, or response-normalization implementation.

The integration test uses an application-supplied `DynamicGraphQLClient` and verifies Query capture
replay plus Command duplicate replay without a second external dispatch. Generated contract checks
verify qualified Block provenance and sanitized connector configuration identity.
