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

## Durable connections through Quarkus OIDC

The optional `host-oidc-quarkus` module integrates Quarkus authorization with durable logical
connections. Quarkus owns redirects, OAuth state, PKCE, code exchange, identity verification and
token-endpoint requests. The host owns application authorization, external account policy,
credentials at rest, database provisioning and client lifetime.

`host-gmail` now supplies only `GmailClients`, an initialized Gmail SDK client factory.
The previous Gmail authorization helper and schema have been removed. Existing installations must
reconnect using the new flow; there is no legacy credential migration or parallel renewal path.

### Configure a dedicated connection flow

Use a named Quarkus OIDC tenant distinct from the application's sign-in tenant. For Gmail:

```properties
quarkus.oidc.gmail.provider=google
quarkus.oidc.gmail.client-id=${GOOGLE_CLIENT_ID}
quarkus.oidc.gmail.credentials.secret=${GOOGLE_CLIENT_SECRET}
quarkus.oidc.gmail.tenant-paths=/connections/gmail/authorize
quarkus.oidc.gmail.authentication.scopes=openid,https://www.googleapis.com/auth/gmail.readonly
quarkus.oidc.gmail.authentication.extra-params.access_type=offline
quarkus.oidc.gmail.authentication.extra-params.prompt=consent
quarkus.oidc.gmail.authentication.pkce-required=true
quarkus.oidc.gmail.authentication.state-secret=${OIDC_STATE_SECRET}
quarkus.oidc.gmail.token-state-manager.strategy=id-token
quarkus.oidc.gmail.token.refresh-expired=false

quarkus.oidc-client.gmail.auth-server-url=https://accounts.google.com
quarkus.oidc-client.gmail.client-id=${GOOGLE_CLIENT_ID}
quarkus.oidc-client.gmail.credentials.secret=${GOOGLE_CLIENT_SECRET}
quarkus.oidc-client.gmail.grant.type=refresh
```

Register the exact HTTPS `/connections/gmail/authorize` URI with the provider. Do not enable
proactive session refresh (`token.refresh-token-time-skew`) for this tenant. Its session retains
only the ID token; durable connection management owns the refresh credentials after handoff.
The completion endpoint removes the temporary connection session through `OidcSession.logout()`.
A stale connection session is cleared before a fresh flow is attempted.
Use the same configuration name for the OIDC tenant and its `OidcClient`, and pass that named
client to the manager. The hooks also guard managed token requests against automatic socket-failure
redispatch: a lost response may already have consumed a code or rotated a refresh token. Quarkus
3.33.1 rejects `connection-retry-count=0` with `maxAttempts must be greater than zero`; the bridge
uses its public request-filter API to reject a retry subscription before it sends another request.
Discovery and application sign-in requests are unaffected. Uncertain renewals require reconnection.

For Microsoft, use `provider=microsoft`, a tenant-specific
`https://login.microsoftonline.com/<tenant-id>/v2.0` authorization server, explicit
`token.issuer` for that tenant, and `openid,offline_access,https://graph.microsoft.com/User.Read` for a read-only Graph proof.
Use the same tenant-specific server for its named `OidcClient`. The Microsoft preset otherwise
allows multiple issuers; the host registration must pin the approved issuer and account policy.
Application tenant, external account and Entra tenant are separate identities.
See the [Quarkus provider guide](https://quarkus.io/guides/security-openid-connect-providers/).

### Assemble host infrastructure

Apply `META-INF/tpf-oidc-connections.sql` explicitly to a dedicated security schema. The SQL is
PostgreSQL/H2 compatible; use the authoritative database on every host, never a lagging replica.
Use a host-managed AES keyring outside the database and a blocking executor.

```java
var registration = new ConnectionRegistration<>(
    "gmail", googleClientId, URI.create("https://accounts.google.com"),
    URI.create("https://app.example/connections/gmail/authorize"),
    Set.of(GmailQueryConnector.REQUIRED_OAUTH_SCOPE),
    AuthenticatedGmailConnection.class,
    verifiedIdentity -> applicationAccountPolicy.permits(verifiedIdentity),
    new GmailClients(hostGoogleTransport, GsonFactory.getDefaultInstance()));

var encryption = new ConnectionEncryption(activeKeyId, hostEncryptionKeys);
var store = new JdbcConnectionStore(dataSource, encryption, registration.storageId());
var connections = new QuarkusConnections(
    store, registration, oidcClients.getClient("gmail"), hostBlockingExecutor, Clock.systemUTC());
```

These types belong to `org.pipelineframework.host.oidc`; `GmailClients` belongs to
`org.pipelineframework.host.gmail`. Publish the manager explicitly through host CDI wiring.

Implement `ConnectionAccess` to authorize Connect, Status and Disconnect against the application's
own identity, returning the permitted `ConnectionKey` and stable actor ID.
Its `authorizeCompletion(RoutingContext)` method must reauthorize the **original application
session**, not the external identity being connected. The completion hook runs before the REST
request scope is active: use its supplied routing context and host session service, not a
request-scoped identity proxy. Never trust an actor header supplied by a browser.
These authorization callbacks run on the request thread and must be nonblocking; use already
validated host session authority there. The bridge offloads its JDBC work separately.

Publish exactly **one** `ConnectionOidcHooks` CDI bean. For one registration construct it with the
manager and access policy. For several registrations pass a list of `ConnectionOidcHooks.Binding`.
Quarkus 3.33.1 skips completion actions when multiple beans make that lookup ambiguous; the bridge
rejects this wiring at startup. Applications with other completion work must compose it through a
single completion action rather than registering competing beans.

Also publish the early token-request filter with the same configuration names:

```java
@Produces
@Singleton
static ConnectionTokenRequests connectionTokenRequests() {
    return new ConnectionTokenRequests(Set.of("gmail"));
}
```

Include every managed name when configuring several registrations. Keep this producer independent
of `OidcClients` and connection managers: Quarkus discovers request filters while constructing those
clients. A static producer, as above, avoids initializing a host bean that injects `OidcClients`.
Startup validation rejects a missing filter or missing managed names.

Subclass `ConnectionResource`, annotate the subclass with the application `@Path`, and inject the
manager and access policy into its constructor. Its protected `authorize` path must match the
registered flow URI and named OIDC tenant. No resource, resolver, security realm, database, key or
SDK transport is installed automatically.

| Endpoint suffix | Behavior |
| --- | --- |
| `POST connect` | Authorizes the host actor, checks browser Origin and starts a ten-minute, one-use connection attempt. |
| `GET authorize` | Quarkus performs code flow; the completion hook commits the verified connection before success. |
| `GET status` | Returns only phase and revision after application authorization. |
| `POST disconnect` | Authorizes the actor, checks Origin and supersedes connection authority. |

A protected HttpOnly connection-attempt cookie binds host authority to Quarkus-generated state.
It does not replace Quarkus's state or PKCE validation. Only one outstanding browser attempt per
registration is supported; a new attempt supersedes the previous attempt and disables its old client
generation. A failed, expired or consumed code flow requires a new Connect action.

Serve management endpoints over HTTPS; HTTP loopback is accepted only for local tests/development.
Configure trusted proxy handling explicitly when TLS terminates upstream. Exclude callback query
strings, Location/Set-Cookie headers and token-endpoint bodies from access logs and tracing.
Management responses are no-store; do not enable cross-origin management access.

### Resolve initialized clients

The application's single `ConnectionResolver` validates the invocation target and reference, then
delegates to the appropriate manager's `resolve(request)`. The manager additionally requires trusted
tenant context and the registered capability type. Pipeline bindings still contain only the logical
connection name.

A client factory receives host-only `RequestAccess`. It must check `accessToken()` immediately before
**every** outgoing request, including previously prepared requests, and must not expose that accessor
through its Connector capability. It must not independently renew credentials or follow untrusted
redirects. `GmailClients` implements these rules through its SDK request interceptor, with retries,
redirects and credential logging disabled.

Resolution and request access consult durable state. Clients are cached by connection generation,
bounded to 128 entries per manager. Refresh revisions do not change the generation; disconnect and
reconnect do. Drain invocations before closing the manager, SDK transports and host executor.
Quarkus owns the named `OidcClient` lifetime. Already-dispatched requests cannot be recalled.
Recorded Query and Command replay continue to bypass live connection resolution.

### Renewal and operational status

The host can schedule `renew(key)` for its authorized connection keys; live resolution uses the same
renewal path. Both offload storage and token requests to the host's blocking executor.
The manager claims renewal through an immutable conditional revision before calling
`OidcClient.refreshTokens`. Concurrent workers wait briefly or receive a temporary-unavailable result.
A missing replacement refresh token preserves the previous token; returned scopes must still satisfy
the registration's required permissions.

- `READY`: locally usable, subject to provider acceptance.
- `CONNECTING`: awaiting completion of a host-authorized connection attempt.
- `REFRESHING`: another worker owns renewal.
- `RENEWAL_UNCERTAIN`: a response or durable update was lost; credentials are unavailable.
- `REQUIRES_REAUTHORIZATION`: interaction/consent is required or the attempt expired.
- `DISCONNECTED`: local authority has been removed.

Explicit temporary token-service errors permit a later retry. Interaction-required errors disable
resolution. Unknown outcomes are not silently retried: they remain distinct from revoked consent.
An abandoned renewal claim becomes uncertain after one minute on the next read. The first bridge
recovers uncertain outcomes through a new authorized Connect action; it does not claim exactly-once
refresh or implement provider-specific recovery policies.

Client factories report resource authentication challenges through `requiresInteraction()`.
The Microsoft fixture proves a bounded Graph read and transition to reauthorization on a claims
challenge, without automatic retries. This is not a claim of full Conditional Access challenge
recovery: forwarding protected claims into a subsequent authorization request is not implemented.

Encrypted payloads are authenticated against connection identity and revision. Successful
transitions delete superseded ciphertext; non-secret revision/time/phase metadata remains for audit.
Keys are host-owned and versioned. Retain old keys until current payloads and retained backups no
longer require them. Hosts own actor-level audit records and credential-retention policy.

Within one registration, verified issuer and subject have one logical owner in the shared store.
Ownership reservations survive disconnect. Transfers require explicit administration; do not create
independent refresh authorities through aliases or another token manager. Connection state is
security infrastructure, separate from authored persistence, Query capture and Command effects.

Disconnect is local. Remote consent revocation can have broader effects and requires a separately
authorized host operation. This integration targets Quarkus 3.33.1; Spring integration and platform
upgrades are separate work. See [ADR-0030](/decisions/0030-optional-host-connection-lifecycle).

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
