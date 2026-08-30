# Host-authenticated Connectors

TPF connectors can consume authenticated external access without implementing authentication
protocols. TPF supplies the semantic invocation context; the application host resolves a logical
connection to a typed, authenticated runtime handle.

```text
TPF Query or Command
        | ConnectorExecutionContext
        v
application ConnectionResolver
        | tenant + ConnectionRef + requested type
        v
host security / connection infrastructure
        | authenticated SDK client
        v
connector -> external system
```

TPF does not authenticate application users, run OAuth/OIDC flows, handle callbacks, store or
refresh tokens, or replace Spring Security, Quarkus security, Keycloak, Auth0, IAM, or a connection
broker.

> **Do not use `SecretRef` or `SecretResolver` for connector authentication.** Those legacy,
> context-free APIs are deprecated for removal. They receive no tenant, execution, connector, or
> operation identity, so they cannot enforce the resolution invariant described here. Existing
> configuration remains readable during migration; new authenticated connectors must use
> `ConnectionRef` and `ConnectionResolutionRequest`.

## Invocation and resolution contracts

For each live native Query or Command, `ConnectorExecutionContext` makes these facts available when
known:

- tenant, execution, pipeline, contract, release, and step identity;
- connector binding plus stable provider and operation identity;
- correlation, trace, and deadline context.

Deadline context is optional. It remains empty when an invocation path has no absolute connector
deadline; TPF does not synthesize one from retry timing or a remote-operator dispatch timeout.

The connector combines that context with its deployment-owned `ConnectionRef`:

```java
ConnectionResolutionRequest<AuthenticatedGmailConnection> request =
    new ConnectionResolutionRequest<>(
        configuration.connection(),
        AuthenticatedGmailConnection.class,
        invocation.executionContext());

CompletionStage<AuthenticatedGmailConnection> connection =
    runtimeContext.connectionResolver().orElseThrow().resolve(request);
```

`ConnectionRef` is an opaque logical binding such as `gmail.primary`. It is configuration, not a
token, email address, or pipeline-supplied account selector. A multi-tenant resolver normally uses
both the reference and `request.invocationContext().tenantId()` to find the connected account.

The resolved object implements `ResolvedConnection`. It is runtime-only and must not enter provider
metadata, pipeline values, Query capture, Command effects, telemetry, or exception messages.

## Quarkus integration

Register at most one CDI `ConnectionResolver` bean. TPF places it in the Quarkus
`ConnectorRuntimeContext`. The application adapter can call its connection service, tenant-aware
token repository, Quarkus REST client/OIDC facilities, or another configured security component and
then construct the provider SDK client.

```java
@ApplicationScoped
class ApplicationConnectionResolver implements ConnectionResolver {
    private final ConnectedGoogleAccounts accounts;

    ApplicationConnectionResolver(ConnectedGoogleAccounts accounts) {
        this.accounts = accounts;
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(
        ConnectionResolutionRequest<C> request
    ) {
        String tenantId = request.invocationContext().tenantId().orElseThrow();
        return accounts.gmailClient(tenantId, request.reference())
            .thenApply(AuthenticatedGmailConnection::new)
            .thenApply(request.connectionType()::cast);
    }
}
```

`ConnectedGoogleAccounts` is application infrastructure. It owns registration, consent, token
persistence, refresh, and SDK credential initialization. TPF deliberately does not provide that
type.

## Spring integration

Register at most one Spring `ConnectionResolver` bean. Spring Boot auto-configuration discovers it
and installs it in `ConnectorRuntimeContext`. An application adapter can use
`OAuth2AuthorizedClientService`, a connection service, or another Spring-managed component.

```java
@Bean
ConnectionResolver applicationConnectionResolver(ConnectedGoogleAccounts accounts) {
    return new ConnectionResolver() {
        @Override
        public <C extends ResolvedConnection> CompletionStage<C> resolve(
            ConnectionResolutionRequest<C> request
        ) {
            String tenantId = request.invocationContext().tenantId().orElseThrow();
            return accounts.gmailClient(tenantId, request.reference())
                .thenApply(AuthenticatedGmailConnection::new)
                .thenApply(request.connectionType()::cast);
        }
    };
}
```

Spring connector step execution is not yet at Quarkus parity. This integration proves the common
resolver and bean-wiring contract; it does not claim full Spring Query/Command connector execution.

Both hosts fail startup when more than one application resolver is visible. If an application needs
multiple credential systems, compose their routing behind one `ConnectionResolver` bean.

## Gmail read-only connector

The `gmail-query-connector` module provides provider `google.gmail` and three cacheable Query
operations:

| Operation | Input | Output |
| --- | --- | --- |
| `list.messages` | `GmailListMessagesRequest` | `GmailMessagePage` |
| `search.messages` | `GmailSearchMessagesRequest` | `GmailMessagePage` |
| `get.message` | `GmailGetMessageRequest` | `GmailMessage` |

Provider configuration is `GmailProviderConfiguration(ConnectionRef connection)`. List and search
use `GmailListMessagesConfiguration` for the result limit and spam/trash inclusion. Inputs contain
message, search, and pagination semantics only; none contains a credential, account identifier, or
connection reference.

The host returns `AuthenticatedGmailConnection`, which wraps an already-authenticated Google
`Gmail` SDK client. The connector requires Google's read-only scope
`https://www.googleapis.com/auth/gmail.readonly`, declared locally as
`GmailQueryConnector.REQUIRED_OAUTH_SCOPE`. There is no portable TPF scope metadata because one
OAuth connector is not enough evidence for a cross-provider authority model.

Query capture is evaluated before live binding activation. Replaying a captured Gmail observation
does not start the provider or call `ConnectionResolver`, so token availability cannot change the
meaning of an already-captured observation.

## OpenAI-compatible LLM connector

Provider `llm.query.openai.compatible` also uses the host-authenticated connection seam. Its provider
configuration contains the model, optional base URL, and an opaque `ConnectionRef`. For every live
Query invocation, it requests an `AuthenticatedOpenAiCompatibleConnection` using the current
`ConnectorExecutionContext`.

The resolved connection exposes a host-supplied factory for an authenticated LangChain4j
`ChatModel`; it does not expose token text through a TPF API. This lets an application resolve API
keys from environment configuration, macOS Keychain, Vault, a cloud secret manager, or another
host facility without encoding that facility in `pipeline.yaml`. Credential rotation takes effect
on the next live invocation because the connector resolves the connection per invocation.

Model and base URL remain connector binding semantics. They identify the external model contract
whose observations TPF captures; they are not credential-selection or security-policy fields.
Captured replay precedes live provider resolution and therefore requires no LLM credential.

## Security boundary

Keep these values out of pipeline inputs and mappings:

- access, refresh, or identity tokens;
- client secrets, passwords, certificates, or API keys;
- connected-account database keys or user email addresses used to select authority;
- provider SDK clients or resolved connection objects.

This contract does not define or enforce whether caller-controlled invocation data may widen a
configured connection's destination. That outbound-authority boundary is unimplemented. Generic
HTTP destinations and centralized outbound authority require a separate
configuration-versus-invocation policy decision. See
[ADR-0021](/decisions/0021-host-owned-connector-authentication).
