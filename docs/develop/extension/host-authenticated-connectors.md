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

The Connector runtime does not authenticate application users, run OAuth/OIDC flows, handle callbacks,
store or refresh tokens, or replace Spring Security, Quarkus security, Keycloak, Auth0, IAM, or a
connection broker. The optional [Gmail host connection library](#durable-gmail-host-connections) assembles
Google's OAuth client and dedicated encrypted storage behind this same runtime boundary.

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
The factory must apply every supplied `ModelConfiguration` setting, including
`strictJsonSchema`. Authentication is host-owned, but the connector's required structured-output
semantics still govern the authenticated model that the host creates.

Model and base URL remain connector binding semantics. They identify the external model contract
whose observations TPF captures; they are not credential-selection or security-policy fields.
Captured replay precedes live provider resolution and therefore requires no LLM credential.

Provider `mcp.client` uses the same seam. The host resolves an initialized MCP client and owns its
transport, session, and lifecycle, including creation and shutdown of any STDIO process. The
connector never persists or closes that handle. See
[Import MCP tools as Connector operations](./mcp-connector-import.md).

## Durable Gmail host connections

The optional `org.pipelineframework:host-gmail` artifact provides a bounded read-only Gmail host
integration. It uses Google's Java OAuth library, verified Google subject identity, a dedicated JDBC
store, and authenticated Gmail clients. It does not install a resolver, REST resource, security realm,
database or encryption key automatically.

Add `host-gmail` at the same version as the framework. The host also needs Quarkus REST Jackson, a
JDBC data source, application authentication and a blocking executor. Apply the packaged
`META-INF/tpf-gmail-connections.sql` migration explicitly to a dedicated security schema. The SQL is
PostgreSQL/H2 compatible; the automated proof uses H2, including a running Quarkus REST application.
Use the authoritative database for every host, never a lagging read replica.

### Construct the host service

The following assembly runs in host infrastructure, outside authored pipeline services:

```java
ConnectionEncryption encryption = new ConnectionEncryption(activeKeyId, hostEncryptionKeys);
JdbcGmailConnectionStore store = new JdbcGmailConnectionStore(dataSource, encryption, googleClientId);
GoogleGmailAuthorization google = new GoogleGmailAuthorization(
    googleClientId, googleClientSecret, URI.create("https://app.example/connections/gmail/callback"),
    GoogleNetHttpTransport.newTrustedTransport(), GsonFactory.getDefaultInstance(), Clock.systemUTC());
GmailConnections connections = new GmailConnections(store, google, hostBlockingExecutor, Clock.systemUTC());
```

`hostEncryptionKeys` is a host-supplied `Map<String, SecretKey>` of AES keys. Use a managed key/secret
facility; do not store keys beside database ciphertext. The active key ID is written into encrypted
envelopes, allowing old keys to remain available while transitions rewrite current payloads with a
new key. Keep old keys until every retained current payload has migrated. Database backups need their
own credential retention and key retirement policy.

The registration ID is the actual OAuth client ID and must match the Google adapter. Configure the
same registration and shared database on all instances. The callback must be an exact registered
HTTPS URI. The library requests `openid` and Gmail read-only access with offline consent and S256
PKCE; callback success requires a verified ID token and the actual Gmail read scope. Client secrets,
OAuth settings, keys and database configuration belong to host deployment configuration.

### Mount authenticated management endpoints

Subclass `GmailConnectionResource` in the application and mount it explicitly:

```java
@Path("/connections/gmail")
public class ApplicationGmailResource extends GmailConnectionResource {
    @Inject
    public ApplicationGmailResource(GmailConnections connections, ApplicationConnectionAccess access) {
        super(connections, access, URI.create("https://app.example"));
    }
}
```

`ApplicationConnectionAccess` implements `GmailConnectionResource.Access`. For each action it must
authorize the authenticated principal to manage a specific tenant's configured logical connection,
then return `Authority(new ConnectionKey(trustedTenant, configuredReference), stableActorId)`.
This is intentionally application policy. Do not derive the tenant or reference from Google's
callback, a submitted account field, or an unverified HTTP header. Callback requests must resume
the same authenticated actor/session and intended logical connection as the connect request.

| Endpoint suffix | Behavior |
| --- | --- |
| `POST connect` | Checks the exact browser `Origin`, creates a ten-minute single-use authorization transaction, and redirects to Google. |
| `GET callback` | Checks authenticated actor, state, browser cookie and expiry before claiming the code exchange. |
| `GET status` | Returns only lifecycle phase and revision after host permission checks. |
| `POST disconnect` | Checks `Origin`, removes usable credentials, invalidates cached access and supersedes outstanding work. |

Serve these endpoints through HTTPS. The browser cookie is Secure, HttpOnly, SameSite=Lax and
host-only. The resource rejects insecure requests and does not enable CORS. If TLS terminates at a
proxy, configure Quarkus to trust forwarded information only from that proxy. Preserve host login
across the Google redirect, and exclude callback query strings and response `Location`/`Set-Cookie`
headers from access logs, tracing exports and reverse-proxy diagnostics. SDK credential request
logging is disabled. Responses use `Cache-Control: no-store` and `Referrer-Policy: no-referrer`.

Only one outstanding browser authorization is supported by the mounted resource's cookie. Starting
another authorization replaces the pending transaction for that logical connection. An invalid
callback does not consume a valid pending transaction. A failed/uncertain code exchange requires a
new connect operation. Expired authorization or exchange/refresh claims are cleared on the next
status or resolution read. No background renewal, cleanup daemon or administrative UI is installed.

### Resolve an authenticated capability

Route Gmail requests through the application's single existing `ConnectionResolver`:

```java
@ApplicationScoped
public class ApplicationConnectionResolver implements ConnectionResolver {
    private final GmailConnections connections;
    private final ApplicationInvocationPolicy policy;

    @Inject
    public ApplicationConnectionResolver(GmailConnections connections, ApplicationInvocationPolicy policy) {
        this.connections = connections;
        this.policy = policy;
    }

    @Override
    public <C extends ResolvedConnection> CompletionStage<C> resolve(ConnectionResolutionRequest<C> request) {
        policy.requireAllowed(request);
        return connections.resolve(request);
    }
}
```

The application policy validates the invocation target and configured reference against host
authority. The helper additionally requires tenant context and the `AuthenticatedGmailConnection`
type. Multiple providers still compose behind one resolver. A binding selects only the logical name:

```yaml
connectors:
  mailbox:
    provider: google.gmail
    version: 1
    config:
      connection: gmail-main
```

Live resolution reads authoritative state every time. Usable clients are cached by tenant, logical
connection and revision, with a maximum of 128 entries per manager. SDK requests check that their
borrowed revision is still usable; they do not perform hidden refresh. Cached Gmail wrappers share
the host transport. Drain invocations before closing `GmailConnections` during application shutdown,
then shut down the host executor. A previously dispatched external request cannot be recalled by
disconnect. Query capture/replay remains unchanged and needs no live connection.

### Renewal, storage and ownership

The manager claims refresh durably before calling Google. Concurrent host instances wait briefly
for the winner or return a bounded temporary-unavailable failure. Missing refresh-token replacements
preserve the current refresh token. Required-scope loss or `invalid_grant` requires reauthorization.
A definite rate limit/temporary rejection permits a later retry. A lost response or otherwise
uncertain refresh is not automatically sent again: the claim remains pending, and after one minute
the next resolution marks the connection as requiring reauthorization. This deliberately conservative
first proof does not implement provider-specific lost-response recovery.

Lifecycle metadata is append-only. Each encrypted payload is bound to connection identity and
revision using AES-GCM. Successful transitions remove older encrypted payloads transactionally;
disconnect retains no grant or pending verifier. Non-secret phase/revision/time metadata remains
for audit. Actor-level access auditing remains part of the host authorization policy.

Within one OAuth registration and shared store, a verified Google account has one logical owner.
Another tenant or alias cannot create an independent renewal loop for the same grant. Ownership
reservations survive disconnect; transferring an account requires explicit host administration.
Do not manage the same grant concurrently in another broker, SDK refresh loop, database, or process.

Disconnect is local. Google grant revocation can affect other scopes and clients in the same project,
so the helper does not call it automatically. Applications requiring remote revocation must expose a
separately authorized operation with appropriate disclosure, tracking and recovery. This release
does not provide a remote revocation queue, Microsoft token-cache adapter, Shopify adapter or
QuickBooks process manager. See [ADR-0030](/decisions/0030-optional-host-connection-lifecycle).

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
