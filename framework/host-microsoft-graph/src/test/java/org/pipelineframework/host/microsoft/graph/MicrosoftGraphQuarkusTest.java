package org.pipelineframework.host.microsoft.graph;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.crypto.spec.SecretKeySpec;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectionResolutionRequest;
import org.pipelineframework.connector.ConnectorExecutionContext;
import org.pipelineframework.host.oidc.ConnectionAccess;
import org.pipelineframework.host.oidc.ConnectionEncryption;
import org.pipelineframework.host.oidc.ConnectionFailure;
import org.pipelineframework.host.oidc.ConnectionKey;
import org.pipelineframework.host.oidc.ConnectionOidcHooks;
import org.pipelineframework.host.oidc.ConnectionRegistration;
import org.pipelineframework.host.oidc.ConnectionResource;
import org.pipelineframework.host.oidc.ConnectionTokenRequests;
import org.pipelineframework.host.oidc.JdbcConnectionStore;
import org.pipelineframework.host.oidc.OidcFixture;
import org.pipelineframework.host.oidc.QuarkusConnections;

/** Actual Microsoft preset/code flow with the production bounded Graph client factory. */
@QuarkusTest
@QuarkusTestResource(MicrosoftGraphQuarkusTest.MicrosoftFixture.class)
class MicrosoftGraphQuarkusTest {
    @TestHTTPResource URI base;
    @Inject QuarkusConnections connections;

    @BeforeEach void resetProvider() { MicrosoftFixture.claimsChallenge(false); }

    @Test void microsoftAuthorizationResolvesGraphAfterBrowserSessionRemoval() throws Exception {
        var cookies = new CookieManager();
        try (var browser = HttpClient.newBuilder().cookieHandler(cookies).followRedirects(HttpClient.Redirect.ALWAYS).build()) {
            var response = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", "http://localhost:8081").header("X-Test-Actor", "alice")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            cookies.getCookieStore().removeAll();
            var invocation = new ConnectorExecutionContext(Optional.of("app-tenant"), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            var request = new ConnectionResolutionRequest<>(new ConnectionRef("microsoft-main"),
                AuthenticatedMicrosoftGraphConnection.class, invocation);
            var graph = connections.resolve(request).toCompletableFuture().join();
            assertEquals("fixture-user", graph.me().id());
            assertEquals("Fixture User", graph.me().displayName().orElseThrow());
            int calls = MicrosoftFixture.graphCalls();
            MicrosoftFixture.claimsChallenge(true);
            assertThrows(ConnectionFailure.class, graph::me);
            assertEquals(calls + 1, MicrosoftFixture.graphCalls());
            assertEquals(QuarkusConnections.Phase.REQUIRES_REAUTHORIZATION,
                connections.status(new ConnectionKey("app-tenant", new ConnectionRef("microsoft-main"))).toCompletableFuture().join().phase());
            connections.disconnect(new ConnectionKey("app-tenant", new ConnectionRef("microsoft-main"))).toCompletableFuture().join();
            MicrosoftFixture.claimsChallenge(false);
            assertThrows(ConnectionFailure.class, graph::me);
        }
    }

    public static final class MicrosoftFixture extends OidcFixture {
        static void claimsChallenge(boolean required) { claimsChallenge.set(required); }
        static int graphCalls() { return graphCalls.get(); }
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
        private final HttpClient http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();
        private Optional<QuarkusConnections> managed = Optional.empty();

        @Produces @Singleton public QuarkusConnections connections() throws Exception {
            var database = new JdbcDataSource();
            database.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            try (var connection = database.getConnection(); var script = new InputStreamReader(
                getClass().getResourceAsStream("/META-INF/tpf-oidc-connections.sql"), StandardCharsets.UTF_8)) {
                RunScript.execute(connection, script);
            }
            var registration = new ConnectionRegistration<>("connection", "test-client", URI.create(issuer),
                URI.create("http://localhost:8081/connections/proof/authorize"), Set.of(MicrosoftGraphClients.USER_READ_SCOPE),
                AuthenticatedMicrosoftGraphConnection.class, identity -> "external-account".equals(identity.getSubject()),
                new MicrosoftGraphClients(http, URI.create(issuer), new ObjectMapper(), Duration.ofSeconds(5)));
            var encryption = new ConnectionEncryption("test", Map.of("test", new SecretKeySpec(new byte[32], "AES")));
            var manager = new QuarkusConnections(new JdbcConnectionStore(database, encryption, registration.storageId()),
                registration, clients.getClient("connection"), executor, Clock.systemUTC());
            managed = Optional.of(manager);
            return manager;
        }

        @Produces @Singleton public ConnectionAccess access() {
            return new ConnectionAccess() {
                private Authority current(RoutingContext context) {
                    if (!"alice".equals(context.request().getHeader("X-Test-Actor"))) {
                        throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
                    }
                    return new Authority(new ConnectionKey("app-tenant", new ConnectionRef("microsoft-main")), "alice");
                }
                @Override public Authority authorize(jakarta.ws.rs.core.SecurityContext security, Action action) { return current(routing); }
                @Override public Authority authorizeCompletion(RoutingContext context) { return current(context); }
            };
        }

        @Produces @Singleton public static ConnectionTokenRequests tokenRequests() {
            return new ConnectionTokenRequests(Set.of("connection"));
        }

        @Produces @Singleton public ConnectionOidcHooks hooks(QuarkusConnections manager, ConnectionAccess access) {
            return new ConnectionOidcHooks(manager, access);
        }

        @PreDestroy void close() { managed.ifPresent(QuarkusConnections::close); http.close(); executor.shutdownNow(); }
    }
}
