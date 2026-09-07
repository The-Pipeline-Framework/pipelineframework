package org.pipelineframework.host.oidc;

import io.quarkus.oidc.client.OidcClients;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import javax.crypto.spec.SecretKeySpec;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.pipelineframework.connector.*;

/** Isolated process fixture. The actor header and expiry endpoint are deliberately test-only. */
@ApplicationScoped
@io.quarkus.arc.properties.IfBuildProperty(name = "test.restart-host", stringValue = "true")
public class RestartHost {
    @Inject OidcClients clients;
    @Inject RoutingContext routing;
    @ConfigProperty(name = "test.oidc.issuer") String issuer;
    @ConfigProperty(name = "test.jdbc.url") String jdbc;
    @ConfigProperty(name = "test.flow.uri") String flow;
    private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(4);
    private final HttpClient http = HttpClient.newHttpClient();
    private Optional<QuarkusConnections> managed = Optional.empty();
    private JdbcConnectionStore store;

    ConnectionKey key() { return new ConnectionKey("app-tenant", new ConnectionRef("restart-main")); }
    JdbcConnectionStore store() { return store; }
    @Produces @Singleton public QuarkusConnections manager() throws Exception {
        var database = new JdbcDataSource(); database.setURL(jdbc);
        try (var connection = database.getConnection(); var script = new InputStreamReader(
            getClass().getResourceAsStream("/META-INF/tpf-oidc-connections.sql"), StandardCharsets.UTF_8)) {
            try (var tables = connection.getMetaData().getTables(connection.getCatalog(), "PUBLIC", "TPF_OIDC_REVISIONS", new String[] {"TABLE"})) {
                if (!tables.next()) { RunScript.execute(connection, script); }
            }
        }
        var registration = new ConnectionRegistration<>("connection", "test-client", URI.create(issuer), URI.create(flow),
            Set.of("https://graph.microsoft.com/User.Read"), Client.class,
            identity -> "external-account".equals(identity.getSubject()), access -> new Client(access, http, URI.create(issuer + "/v1.0/me")));
        store = new JdbcConnectionStore(database, new ConnectionEncryption("test",
            Map.of("test", new SecretKeySpec(new byte[32], "AES"))), registration.storageId());
        var result = new QuarkusConnections(store, registration, clients.getClient("connection"), executor, Clock.systemUTC());
        managed = Optional.of(result); return result;
    }
    @Produces @Singleton public ConnectionAccess access() {
        return new ConnectionAccess() {
            private Authority current(RoutingContext context) {
                if (!"alice".equals(context.request().getHeader("X-Test-Actor"))) {
                    throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
                }
                return new Authority(key(), "alice");
            }
            @Override public Authority authorize(jakarta.ws.rs.core.SecurityContext security, Action action) { return current(routing); }
            @Override public Authority authorizeCompletion(RoutingContext context) { return current(context); }
        };
    }
    @Produces @Singleton public static ConnectionTokenRequests tokenRequests() { return new ConnectionTokenRequests(Set.of("connection")); }
    @Produces @Singleton public ConnectionOidcHooks hooks(QuarkusConnections connections, ConnectionAccess access) {
        return new ConnectionOidcHooks(connections, access);
    }
    @PreDestroy void close() { managed.ifPresent(QuarkusConnections::close); http.close(); executor.shutdownNow(); }

    public static final class Client implements ResolvedConnection {
        private final ConnectionRegistration.RequestAccess access;
        private final HttpClient http;
        private final URI endpoint;
        Client(ConnectionRegistration.RequestAccess access, HttpClient http, URI endpoint) {
            this.access = access; this.http = http; this.endpoint = endpoint;
        }
        String me() throws Exception {
            var response = http.send(HttpRequest.newBuilder(endpoint).header("Authorization", "Bearer " + access.accessToken())
                .timeout(java.time.Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) { throw new IllegalStateException("Graph fixture rejected request"); }
            return response.body();
        }
    }
    @Path("/connections/proof")
    @io.quarkus.arc.properties.IfBuildProperty(name = "test.restart-host", stringValue = "true")
    public static class Mounted extends ConnectionResource {
        @Inject public Mounted(QuarkusConnections connections, ConnectionAccess access) { super(connections, access); }
    }
    @Path("/proof")
    @io.quarkus.arc.properties.IfBuildProperty(name = "test.restart-host", stringValue = "true")
    public static class Probe {
        @Inject RestartHost host;
        @Inject QuarkusConnections connections;
        @GET @Path("me") public String me() throws Exception {
            var context = new ConnectorExecutionContext(Optional.of(host.key().tenant()), Optional.empty(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
            return connections.resolve(new ConnectionResolutionRequest<>(host.key().reference(), Client.class, context))
                .toCompletableFuture().join().me();
        }
        @POST @Path("expire") public void expire() {
            var state = host.store().read(host.key()).orElseThrow(); var grant = state.grant().orElseThrow();
            if (!host.store().append(host.key(), state.next(QuarkusConnections.Phase.READY, System.currentTimeMillis(),
                Optional.of(new ConnectionState.Grant(grant.account(), grant.accessToken(), grant.refreshToken(), 1, grant.scopes())), Optional.empty()))) {
                throw new IllegalStateException("Cannot expire fixture credentials");
            }
        }
    }
}
