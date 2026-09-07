package org.pipelineframework.host.oidc;

import static org.junit.jupiter.api.Assertions.*;

import io.quarkus.oidc.client.OidcClients;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Path;
import java.io.InputStreamReader;
import java.net.CookieManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.crypto.spec.SecretKeySpec;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.pipelineframework.connector.*;

@QuarkusTest
@QuarkusTestResource(OidcFixture.class)
class ConnectionBridgeTest {
    @TestHTTPResource URI base;
    @Inject QuarkusConnections connections;
    @Inject Host host;

    @BeforeEach void resetProvider() {
        OidcFixture.subject.set("external-account");
        OidcFixture.scope.set("https://graph.microsoft.com/User.Read");
        OidcFixture.tokenIssuer.set("");
        OidcFixture.refreshError.set("");
        OidcFixture.rotate.set(false);
        OidcFixture.loseRefreshResponse.set(false);
        OidcFixture.claimsChallenge.set(false);
        OidcFixture.refreshEntered.set(Optional.empty());
        OidcFixture.refreshRelease.set(Optional.empty());
        connections.disconnect(key()).toCompletableFuture().join();
    }

    @Test
    void durableConnectionSurvivesBrowserRemovalAndManagerRestartThenDisconnectFencesOldClient() throws Exception {
        var cookies = new CookieManager();
        try (var browser = HttpClient.newBuilder().cookieHandler(cookies).followRedirects(HttpClient.Redirect.ALWAYS).build()) {
            var response = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", origin()).header("X-Test-Actor", "alice").POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            var trace = new java.util.ArrayList<String>();
            Optional<? extends HttpResponse<?>> hop = Optional.of(response);
            while (hop.isPresent()) {
                trace.add(hop.orElseThrow().uri().getPath() + "=" + hop.orElseThrow().statusCode());
                hop = hop.orElseThrow().previousResponse();
            }
            assertEquals(200, response.statusCode(), trace + " " + connections.status(key()).toCompletableFuture().join());
            assertEquals(QuarkusConnections.Phase.READY, connections.status(key()).toCompletableFuture().join().phase());
            assertTrue(cookies.getCookieStore().getCookies().stream().noneMatch(cookie -> cookie.getName().startsWith("q_session")));
            cookies.getCookieStore().removeAll();
            try (var restarted = host.newManager()) {
                var capability = restarted.resolve(request()).toCompletableFuture().join();
                assertEquals("fixture-user", capability.me());
                // Expire only access credentials; the real Quarkus client must perform renewal.
                var previous = host.store().read(key()).orElseThrow();
                var grant = previous.grant().orElseThrow();
                var expired = new ConnectionState.Grant(grant.account(), grant.accessToken(), grant.refreshToken(), 1, grant.scopes());
                assertTrue(host.store().append(key(), previous.next(QuarkusConnections.Phase.READY, System.currentTimeMillis(), Optional.of(expired), Optional.empty())));
                int refreshedBefore = OidcFixture.refreshes.get();
                assertEquals("fixture-user", capability.me());
                assertEquals(refreshedBefore + 1, OidcFixture.refreshes.get());
                assertEquals("fixture-refresh", host.store().read(key()).orElseThrow().grant().orElseThrow().refreshToken());
                connections.disconnect(key()).toCompletableFuture().join();
                assertThrows(ConnectionFailure.class, capability::me);
                assertThrows(java.util.concurrent.CompletionException.class, () -> restarted.resolve(request()).toCompletableFuture().join());
            }
        }
    }

    @Test
    void managementRequiresHostAuthorityAndSameOrigin() throws Exception {
        assertEquals("https://example.com", ConnectionResource.normalizedOrigin(URI.create("HTTPS://Example.COM:443/path")));
        assertEquals("http://example.com:8080", ConnectionResource.normalizedOrigin(URI.create("http://Example.COM:8080/path")));
        try (var browser = HttpClient.newHttpClient()) {
            var denied = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", origin()).POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(403, denied.statusCode());
            var crossOrigin = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", "https://evil.invalid").header("X-Test-Actor", "alice")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(403, crossOrigin.statusCode());
        }
    }

    @Test void invalidStateAndWrongBrowserCannotComplete() throws Exception {
        try (var browser = browser(false)) {
            URI callback = callback(browser);
            URI invalid = URI.create(callback.toString().replaceFirst("state=[^&]+", "state=invalid"));
            assertNotEquals(200, get(browser, invalid, "alice").statusCode());
            try (var stranger = browser(false)) { assertNotEquals(200, get(stranger, callback, "alice").statusCode()); }
            assertEquals(QuarkusConnections.Phase.CONNECTING, connections.status(key()).toCompletableFuture().join().phase());
        }
    }

    @Test void changedHostActorCannotClaimTheAttempt() throws Exception {
        try (var browser = browser(false)) {
            URI callback = callback(browser);
            assertEquals(401, get(browser, callback, "bob").statusCode());
            assertNotEquals(QuarkusConnections.Phase.READY, connections.status(key()).toCompletableFuture().join().phase());
        }
    }

    @Test void expiredAndSupersededAttemptsAreRejected() throws Exception {
        try (var browser = browser(false)) {
            URI callback = callback(browser);
            connections.begin(key(), "alice").toCompletableFuture().join();
            assertEquals(401, get(browser, callback, "alice").statusCode());
            var pending = host.store().read(key()).orElseThrow();
            var challenge = pending.challenge().orElseThrow();
            assertTrue(host.store().append(key(), pending.next(QuarkusConnections.Phase.CONNECTING, System.currentTimeMillis(),
                Optional.empty(), Optional.of(new ConnectionState.Challenge(challenge.browserHash(), challenge.actor(), 1)))));
            assertEquals(QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION, connections.status(key()).toCompletableFuture().join().phase());
        }
    }

    @Test void wrongExternalAccountIssuerAndScopesNeverBecomeResolvable() throws Exception {
        for (String defect : java.util.List.of("account", "issuer", "scopes")) {
            OidcFixture.subject.set(defect.equals("account") ? "other-account" : "external-account");
            OidcFixture.tokenIssuer.set(defect.equals("issuer") ? "https://wrong-issuer.invalid" : "");
            OidcFixture.scope.set(defect.equals("scopes") ? "openid" : "https://graph.microsoft.com/User.Read");
            try (var browser = browser(true)) {
                assertEquals(401, connect(browser).statusCode(), defect);
                assertThrows(java.util.concurrent.CompletionException.class, () -> connections.resolve(request()).toCompletableFuture().join());
            }
        }
    }

    @Test void callbackReplayAndCrossTenantResolutionAreRejected() throws Exception {
        try (var browser = browser(false)) {
            URI callback = callback(browser);
            var completed = get(browser, callback, "alice");
            assertEquals(302, completed.statusCode());
            assertEquals(200, get(browser, URI.create(completed.headers().firstValue("Location").orElseThrow()), "alice").statusCode());
            assertNotEquals(200, get(browser, callback, "alice").statusCode());
            var wrongTenant = new ConnectionResolutionRequest<>(key().reference(), Capability.class,
                new ConnectorExecutionContext(Optional.of("different-tenant"), Optional.empty(), Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
            assertThrows(java.util.concurrent.CompletionException.class, () -> connections.resolve(wrongTenant).toCompletableFuture().join());
            var account = host.store().read(key()).orElseThrow().grant().orElseThrow().account();
            assertThrows(ConnectionFailure.class, () -> host.store().claimAccount(new ConnectionKey("different-tenant", key().reference()), account));
        }
    }

    @Test void concurrentWorkersRefreshOnceAndPersistRotation() throws Exception {
        authorize(); expire();
        OidcFixture.rotate.set(true);
        int before = OidcFixture.refreshes.get();
        try (var another = host.newManager()) {
            var first = connections.renew(key()).toCompletableFuture();
            var second = another.resolve(request()).toCompletableFuture();
            assertEquals(QuarkusConnections.Phase.READY, first.join().phase());
            assertEquals("fixture-user", second.join().me());
        }
        assertEquals(before + 1, OidcFixture.refreshes.get());
        assertEquals("rotated-refresh", host.store().read(key()).orElseThrow().grant().orElseThrow().refreshToken());
    }

    @Test void disconnectFencesAnInflightRenewal() throws Exception {
        authorize(); expire();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        OidcFixture.refreshEntered.set(Optional.of(entered));
        OidcFixture.refreshRelease.set(Optional.of(release));
        var pending = connections.resolve(request()).toCompletableFuture();
        try {
            assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
            connections.disconnect(key()).toCompletableFuture().join();
        } finally { release.countDown(); }
        assertThrows(java.util.concurrent.CompletionException.class, pending::join);
        assertEquals(QuarkusConnections.Phase.DISCONNECTED, connections.status(key()).toCompletableFuture().join().phase());
    }

    @Test void reconnectSupersedesAnInflightRenewalAndRetainedClients() throws Exception {
        authorize();
        var retained = connections.resolve(request()).toCompletableFuture().join();
        expire();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        OidcFixture.refreshEntered.set(Optional.of(entered));
        OidcFixture.refreshRelease.set(Optional.of(release));
        var pending = connections.renew(key()).toCompletableFuture();
        try {
            assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
            connections.disconnect(key()).toCompletableFuture().join();
            authorize();
        } finally { release.countDown(); }
        assertThrows(java.util.concurrent.CompletionException.class, pending::join);
        assertEquals(QuarkusConnections.Phase.READY, connections.status(key()).toCompletableFuture().join().phase());
        assertEquals("initial-access", host.store().read(key()).orElseThrow().grant().orElseThrow().accessToken());
        assertThrows(ConnectionFailure.class, retained::me);
        assertEquals("fixture-user", connections.resolve(request()).toCompletableFuture().join().me());
    }

    @Test void refreshFailuresDistinguishRetryReauthorizationAndUncertainty() throws Exception {
        for (var entry : Map.of("temporarily_unavailable", QuarkusConnections.Phase.READY,
            "invalid_grant", QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION,
            "interaction_required", QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION,
            "unknown_provider_failure", QuarkusConnections.Phase.RENEWAL_UNCERTAIN).entrySet()) {
            authorize(); expire();
            OidcFixture.refreshError.set(entry.getKey());
            assertThrows(java.util.concurrent.CompletionException.class, () -> connections.resolve(request()).toCompletableFuture().join());
            assertEquals(entry.getValue(), connections.status(key()).toCompletableFuture().join().phase());
            OidcFixture.refreshError.set("");
        }
    }

    @Test void lostRefreshResponseIsNotRetriedByQuarkusOrAnotherWorker() throws Exception {
        authorize(); expire();
        int before = OidcFixture.refreshes.get();
        OidcFixture.loseRefreshResponse.set(true);
        assertThrows(java.util.concurrent.CompletionException.class, () -> connections.renew(key()).toCompletableFuture().join());
        assertEquals(QuarkusConnections.Phase.RENEWAL_UNCERTAIN, connections.status(key()).toCompletableFuture().join().phase());
        try (var another = host.newManager()) {
            assertThrows(java.util.concurrent.CompletionException.class, () -> another.resolve(request()).toCompletableFuture().join());
        }
        assertEquals(before + 1, OidcFixture.refreshes.get());
    }

    @Test void completionCannotReportSuccessBeforeItsDurableCommit() throws Exception {
        try (var browser = browser(false)) {
            URI callback = callback(browser);
            long rejectedRevision = host.store().read(key()).orElseThrow().revision() + 1;
            host.sql("ALTER TABLE tpf_oidc_payloads ADD CONSTRAINT reject_completion CHECK (revision <> " + rejectedRevision + ")");
            try {
                assertEquals(401, get(browser, callback, "alice").statusCode());
                assertNotEquals(QuarkusConnections.Phase.READY, connections.status(key()).toCompletableFuture().join().phase());
                assertThrows(java.util.concurrent.CompletionException.class, () -> connections.resolve(request()).toCompletableFuture().join());
            } finally { host.sql("ALTER TABLE tpf_oidc_payloads DROP CONSTRAINT reject_completion"); }
        }
    }

    @Test void failedDurableRenewalCannotReturnANewCapability() throws Exception {
        authorize(); expire();
        long rejectedRevision = host.store().read(key()).orElseThrow().revision() + 2;
        host.sql("ALTER TABLE tpf_oidc_payloads ADD CONSTRAINT reject_renewal CHECK (revision <> " + rejectedRevision + ")");
        try {
            assertThrows(java.util.concurrent.CompletionException.class, () -> connections.resolve(request()).toCompletableFuture().join());
            assertEquals(QuarkusConnections.Phase.REFRESHING, host.store().read(key()).orElseThrow().phase());
        } finally { host.sql("ALTER TABLE tpf_oidc_payloads DROP CONSTRAINT reject_renewal"); }
        var abandoned = host.store().read(key()).orElseThrow();
        assertTrue(host.store().append(key(), abandoned.next(QuarkusConnections.Phase.REFRESHING, System.currentTimeMillis() - 61000,
            abandoned.grant(), Optional.empty())));
        assertEquals(QuarkusConnections.Phase.RENEWAL_UNCERTAIN, connections.status(key()).toCompletableFuture().join().phase());
    }

    @Test void graphClaimsChallengeRequiresInteractionWithoutRetry() throws Exception {
        authorize();
        var capability = connections.resolve(request()).toCompletableFuture().join();
        OidcFixture.claimsChallenge.set(true);
        int before = OidcFixture.graphCalls.get();
        assertThrows(ConnectionFailure.class, capability::me);
        assertEquals(before + 1, OidcFixture.graphCalls.get());
        assertEquals(QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION, connections.status(key()).toCompletableFuture().join().phase());
    }

    private HttpClient browser(boolean follow) {
        return HttpClient.newBuilder().cookieHandler(new CookieManager())
            .followRedirects(follow ? HttpClient.Redirect.ALWAYS : HttpClient.Redirect.NEVER).build();
    }
    private HttpResponse<String> connect(HttpClient browser) throws Exception {
        return browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect")).header("Origin", origin())
            .header("X-Test-Actor", "alice").POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
    }
    private HttpResponse<String> get(HttpClient browser, URI uri, String actor) throws Exception {
        return browser.send(HttpRequest.newBuilder(uri).header("X-Test-Actor", actor).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
    private URI callback(HttpClient browser) throws Exception {
        var start = connect(browser);
        assertEquals(303, start.statusCode());
        var redirect = get(browser, URI.create(start.headers().firstValue("Location").orElseThrow()), "alice");
        assertEquals(302, redirect.statusCode());
        var provider = get(browser, URI.create(redirect.headers().firstValue("Location").orElseThrow()), "alice");
        assertEquals(302, provider.statusCode());
        return URI.create(provider.headers().firstValue("Location").orElseThrow());
    }
    private void authorize() throws Exception {
        try (var browser = browser(true)) { assertEquals(200, connect(browser).statusCode()); }
    }
    private void expire() {
        var previous = host.store().read(key()).orElseThrow();
        var grant = previous.grant().orElseThrow();
        assertTrue(host.store().append(key(), previous.next(QuarkusConnections.Phase.READY, System.currentTimeMillis(),
            Optional.of(new ConnectionState.Grant(grant.account(), grant.accessToken(), grant.refreshToken(), 1, grant.scopes())), Optional.empty())));
    }

    private String origin() { return base.getScheme() + "://" + base.getRawAuthority(); }
    private ConnectionKey key() { return new ConnectionKey("app-tenant", new ConnectionRef("proof-main")); }
    private ConnectionResolutionRequest<Capability> request() {
        return new ConnectionResolutionRequest<>(key().reference(), Capability.class,
            new ConnectorExecutionContext(Optional.of(key().tenant()), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()));
    }
    /** Test-only Graph client factory proof, with no credential accessor in its runtime contract. */
    static final class Capability implements ResolvedConnection {
        private final ConnectionRegistration.RequestAccess access;
        private final URI endpoint;
        private final HttpClient http;
        Capability(ConnectionRegistration.RequestAccess access, URI endpoint, HttpClient http) {
            this.access = access; this.endpoint = endpoint; this.http = http;
        }
        String me() throws Exception {
            var response = http.send(HttpRequest.newBuilder(endpoint).header("Authorization", "Bearer " + access.accessToken())
                .GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 401) {
                access.requiresInteraction();
                throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE);
            }
            if (response.statusCode() != 200) { throw new IllegalStateException("Graph request failed"); }
            return new com.fasterxml.jackson.databind.ObjectMapper().readTree(response.body()).path("id").asText();
        }
    }

    @Path("/connections/proof")
    public static class Mounted extends ConnectionResource {
        @Inject public Mounted(QuarkusConnections connections, ConnectionAccess access) { super(connections, access); }
    }

    @ApplicationScoped
    public static class Host {
        @Inject OidcClients clients;
        @Inject RoutingContext routing;
        @ConfigProperty(name = "test.oidc.issuer") String issuer;
        private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(4);
        private final JdbcDataSource database = new JdbcDataSource();
        private final HttpClient graph = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
        private Optional<QuarkusConnections> managed = Optional.empty();
        JdbcConnectionStore store;
        ConnectionRegistration<Capability> registration;

        @Produces @Singleton
        public QuarkusConnections manager() throws Exception {
            database.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            try (var connection = database.getConnection(); var script = new InputStreamReader(
                getClass().getResourceAsStream("/META-INF/tpf-oidc-connections.sql"), StandardCharsets.UTF_8)) {
                RunScript.execute(connection, script);
            }
            registration = new ConnectionRegistration<>("connection", "test-client", URI.create(issuer),
                URI.create("http://localhost:8081/connections/proof/authorize"), Set.of("https://graph.microsoft.com/User.Read"), Capability.class,
                identity -> "external-account".equals(identity.getSubject()),
                access -> new Capability(access, URI.create(issuer + "/v1.0/me"), graph));
            var encryption = new ConnectionEncryption("test", Map.of("test", new SecretKeySpec(new byte[32], "AES")));
            store = new JdbcConnectionStore(database, encryption, registration.storageId());
            var instance = newManager();
            managed = Optional.of(instance);
            return instance;
        }
        QuarkusConnections newManager() {
            return new QuarkusConnections(store, registration, clients.getClient("connection"), executor, Clock.systemUTC());
        }
        JdbcConnectionStore store() { return store; }
        void sql(String sql) throws Exception {
            try (var connection = database.getConnection(); var statement = connection.createStatement()) { statement.execute(sql); }
        }
        @Produces @Singleton
        public ConnectionAccess access() {
            return new ConnectionAccess() {
                // Only the APPLICATION identity is stubbed. External authentication is real Quarkus OIDC.
                private Authority current(RoutingContext context) {
                    String actor = context.request().getHeader("X-Test-Actor");
                    if (!Set.of("alice", "bob").contains(actor == null ? "" : actor)) {
                        throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
                    }
                    return new Authority(new ConnectionKey("app-tenant", new ConnectionRef("proof-main")), actor);
                }
                @Override public Authority authorize(jakarta.ws.rs.core.SecurityContext security, Action action) { return current(routing); }
                @Override public Authority authorizeCompletion(RoutingContext context) { return current(context); }
            };
        }
        @Produces @Singleton
        public static ConnectionTokenRequests tokenRequests() { return new ConnectionTokenRequests(Set.of("connection")); }
        @Produces @Singleton
        public ConnectionOidcHooks hooks(QuarkusConnections manager, ConnectionAccess access) {
            return new ConnectionOidcHooks(manager, access);
        }
        @PreDestroy void close() { managed.ifPresent(QuarkusConnections::close); graph.close(); executor.shutdownNow(); }
    }
}
