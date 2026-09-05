package org.pipelineframework.host.gmail;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import javax.crypto.spec.SecretKeySpec;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Path;
import org.h2.jdbcx.JdbcDataSource;
import org.h2.tools.RunScript;
import org.junit.jupiter.api.Test;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;
import org.pipelineframework.connector.ConnectionRef;

/** Boots Quarkus REST and the actual encrypted JDBC lifecycle, with only the Google service faked. */
@QuarkusTest
class GmailConnectionQuarkusTest {
    @Inject
    org.pipelineframework.connector.ConnectorRuntimeContext runtimeContext;
    @Test
    void unauthenticatedHostCannotManageConnections() {
        given().header("X-Forwarded-Proto", "https").get("/connections/gmail/status")
            .then().statusCode(403).header("Cache-Control", "no-store");
    }

    @Test
    @TestSecurity(user = "alice", roles = "connection-manager")
    void browserFlowRequiresOriginAndCookieThenDisconnects() {
        given().header("X-Forwarded-Proto", "https").header("Origin", "https://evil.example")
            .post("/connections/gmail/connect").then().statusCode(403);
        var start = given().redirects().follow(false).header("X-Forwarded-Proto", "https")
            .header("Origin", "https://host.example").post("/connections/gmail/connect")
            .then().statusCode(303).header("Cache-Control", "no-store")
            .header("Set-Cookie", allOf(containsString("Secure"), containsString("HttpOnly"), containsString("SameSite=Lax")))
            .extract().response();
        String state = URI.create(start.header("Location")).getRawQuery().substring(6);
        given().header("X-Forwarded-Proto", "https").queryParam("state", state).queryParam("code", "account-a")
            .get("/connections/gmail/callback").then().statusCode(400);
        given().header("X-Forwarded-Proto", "https").cookie("__Host-tpf-gmail-oauth", start.cookie("__Host-tpf-gmail-oauth"))
            .queryParam("state", state).queryParam("code", "account-a")
            .get("/connections/gmail/callback").then().statusCode(200).body("phase", equalTo("READY"))
            .body(not(containsString("secret"))).body(not(containsString("account-a")));
        given().header("X-Forwarded-Proto", "https").get("/connections/gmail/status")
            .then().statusCode(200).body("phase", equalTo("READY"));
        var invocation = new org.pipelineframework.connector.ConnectorExecutionContext(java.util.Optional.of("alice"),
            java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(),
            java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty(), java.util.Optional.empty());
        var request = new org.pipelineframework.connector.ConnectionResolutionRequest<>(new ConnectionRef("gmail-main"),
            org.pipelineframework.connector.gmail.AuthenticatedGmailConnection.class, invocation);
        org.junit.jupiter.api.Assertions.assertNotNull(runtimeContext.connectionResolver().orElseThrow()
            .resolve(request).toCompletableFuture().join().client());
        given().header("X-Forwarded-Proto", "https").header("Origin", "https://host.example")
            .post("/connections/gmail/disconnect").then().statusCode(200).body("phase", equalTo("DISCONNECTED"));
    }

    @Test
    @TestSecurity(user = "broken", roles = "connection-manager")
    void unexpectedHostFailureIsServerErrorWithoutSensitiveResponse() {
        var records = new java.util.concurrent.CopyOnWriteArrayList<java.util.logging.LogRecord>();
        var logger = java.util.logging.Logger.getLogger(GmailConnectionResource.class.getName());
        var handler = new java.util.logging.Handler() {
            @Override public void publish(java.util.logging.LogRecord record) { records.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(handler);
        try {
            given().header("X-Forwarded-Proto", "https").get("/connections/gmail/status")
                .then().statusCode(500).header("Cache-Control", "no-store").body(equalTo(""));
            org.junit.jupiter.api.Assertions.assertEquals(1, records.size());
            var record = records.getFirst();
            org.junit.jupiter.api.Assertions.assertNull(record.getThrown());
            org.junit.jupiter.api.Assertions.assertArrayEquals(new Object[] {GmailConnectionResource.Action.STATUS,
                IllegalStateException.class.getName()}, record.getParameters());
        } finally {
            logger.removeHandler(handler);
        }
    }

    @Test
    @TestSecurity(user = "visitor", roles = "viewer")
    void applicationManagementPolicyIsRequired() {
        given().header("X-Forwarded-Proto", "https").get("/connections/gmail/status").then().statusCode(403);
    }

    @Path("/connections/gmail")
    public static class MountedGmailResource extends GmailConnectionResource {
        @Inject
        public MountedGmailResource(GmailConnections connections) {
            super(connections, (security, action) -> {
                if (!security.isUserInRole("connection-manager")) {
                    throw new ConnectionFailure(ConnectionFailure.Reason.FORBIDDEN);
                }
                // This proof uses one tenant per authenticated principal. No request field selects it.
                String actor = security.getUserPrincipal().getName();
                if (actor.equals("broken")) { throw new IllegalStateException("private diagnostic must not be logged"); }
                return new Authority(new ConnectionKey(actor, new ConnectionRef("gmail-main")), actor);
            }, URI.create("https://host.example"));
        }
    }

    @ApplicationScoped
    public static class HostBeans {
        private final java.util.concurrent.ExecutorService executor = Executors.newFixedThreadPool(4);
        private GmailConnections manager;

        @Produces
        @Singleton
        public GmailConnections connections() throws Exception {
            JdbcDataSource database = new JdbcDataSource();
            database.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
            try (var connection = database.getConnection(); var script = new InputStreamReader(
                getClass().getResourceAsStream("/META-INF/tpf-gmail-connections.sql"), StandardCharsets.UTF_8)) {
                RunScript.execute(connection, script);
            }
            var encryption = new ConnectionEncryption("test-key", Map.of("test-key", new SecretKeySpec(new byte[32], "AES")));
            manager = new GmailConnections(new JdbcGmailConnectionStore(database, encryption, "test-client-id"),
                new GmailConnectionsTest.FakeAuthorization(Clock.systemUTC()), executor, Clock.systemUTC());
            return manager;
        }

        @PreDestroy
        void stop() {
            java.util.Optional.ofNullable(manager).ifPresent(GmailConnections::close);
            executor.shutdownNow();
        }

        @Produces
        @Singleton
        public org.pipelineframework.connector.ConnectionResolver resolver(GmailConnections connections) {
            return new org.pipelineframework.connector.ConnectionResolver() {
                @Override
                public <C extends org.pipelineframework.connector.ResolvedConnection> java.util.concurrent.CompletionStage<C> resolve(
                    org.pipelineframework.connector.ConnectionResolutionRequest<C> request) {
                    return connections.resolve(request);
                }
            };
        }
    }
}
