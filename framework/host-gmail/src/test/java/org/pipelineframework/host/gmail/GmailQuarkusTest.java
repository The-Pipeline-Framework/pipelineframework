package org.pipelineframework.host.gmail;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
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
import java.util.HashMap;
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
import org.pipelineframework.connector.*;
import org.pipelineframework.connector.gmail.*;
import org.pipelineframework.host.oidc.*;

/** Actual Google preset/code flow and replacement Gmail factory. Only external servers and app identity are fixtures. */
@QuarkusTest
@QuarkusTestResource(GmailQuarkusTest.GoogleFixture.class)
class GmailQuarkusTest {
    @TestHTTPResource URI base;
    @Inject QuarkusConnections connections;
    @Test void googleAuthorizationResolvesTheGmailSdkWithoutBrowserSession() throws Exception {
        var cookies = new CookieManager();
        try (var browser = HttpClient.newBuilder().cookieHandler(cookies).followRedirects(HttpClient.Redirect.ALWAYS).build()) {
            var response = browser.send(HttpRequest.newBuilder(base.resolve("connections/proof/connect"))
                .header("Origin", "http://localhost:8081").header("X-Test-Actor", "alice")
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            cookies.getCookieStore().removeAll();
            var invocation = new ConnectorExecutionContext(Optional.of("app-tenant"), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            var request = new ConnectionResolutionRequest<>(new ConnectionRef("gmail-main"), AuthenticatedGmailConnection.class, invocation);
            var gmail = connections.resolve(request).toCompletableFuture().join();
            assertEquals("message-1", gmail.client().users().messages().list("me").execute().getMessages().getFirst().getId());
            connections.disconnect(new ConnectionKey("app-tenant", new ConnectionRef("gmail-main"))).toCompletableFuture().join();
            assertThrows(ConnectionFailure.class, () -> gmail.client().users().messages().list("me").execute());
        }
    }
    public static class GoogleFixture extends OidcFixture {
        @Override public Map<String, String> start() {
            scope.set(GmailQueryConnector.REQUIRED_OAUTH_SCOPE);
            var config = new HashMap<>(super.start());
            config.put("quarkus.oidc.connection.provider", "google");
            config.put("quarkus.oidc.connection.authentication.scopes", "openid," + GmailQueryConnector.REQUIRED_OAUTH_SCOPE);
            config.put("quarkus.oidc.connection.authentication.extra-params.access_type", "offline");
            config.put("quarkus.oidc.connection.authentication.extra-params.prompt", "consent");
            return config;
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
        private Optional<QuarkusConnections> managed = Optional.empty();
        private final MockHttpTransport transport = new MockHttpTransport.Builder().setLowLevelHttpResponse(
            new MockLowLevelHttpResponse().setStatusCode(200).setContentType("application/json")
                .setContent("{\"messages\":[{\"id\":\"message-1\"}]}")).build();
        @Produces @Singleton public QuarkusConnections connections() throws Exception {
            var database = new JdbcDataSource();
            database.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            try (var connection = database.getConnection(); var script = new InputStreamReader(
                getClass().getResourceAsStream("/META-INF/tpf-oidc-connections.sql"), StandardCharsets.UTF_8)) {
                RunScript.execute(connection, script);
            }
            var registration = new ConnectionRegistration<>("connection", "test-client", URI.create(issuer),
                URI.create("http://localhost:8081/connections/proof/authorize"), Set.of(GmailQueryConnector.REQUIRED_OAUTH_SCOPE),
                AuthenticatedGmailConnection.class, identity -> "external-account".equals(identity.getSubject()),
                new GmailClients(transport, GsonFactory.getDefaultInstance()));
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
                    return new Authority(new ConnectionKey("app-tenant", new ConnectionRef("gmail-main")), "alice");
                }
                @Override public Authority authorize(jakarta.ws.rs.core.SecurityContext security, Action action) { return current(routing); }
                @Override public Authority authorizeCompletion(RoutingContext context) { return current(context); }
            };
        }
        @Produces @Singleton public static ConnectionTokenRequests tokenRequests() { return new ConnectionTokenRequests(Set.of("connection")); }
        @Produces @Singleton public ConnectionOidcHooks hooks(QuarkusConnections manager, ConnectionAccess access) {
            return new ConnectionOidcHooks(manager, access);
        }
        @PreDestroy void close() throws Exception { managed.ifPresent(QuarkusConnections::close); transport.shutdown(); executor.shutdownNow(); }
    }
}
