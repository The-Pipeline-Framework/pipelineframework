package org.pipelineframework.host.microsoft.graph;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.host.oidc.ConnectionFailure;
import org.pipelineframework.host.oidc.ConnectionRegistration;

class MicrosoftGraphClientsTest {
    private HttpServer graph;
    private final AtomicInteger calls = new AtomicInteger();
    private final AtomicInteger status = new AtomicInteger(200);
    private final AtomicReference<String> body = new AtomicReference<>(
        "{\"id\":\"account-1\",\"displayName\":\"Ada Lovelace\",\"userPrincipalName\":\"ada@example.test\"}");
    private final AtomicReference<String> authorization = new AtomicReference<>("");

    @BeforeEach void start() throws Exception {
        graph = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        graph.createContext("/v1.0/me", request -> {
            calls.incrementAndGet();
            authorization.set(request.getRequestHeaders().getFirst("Authorization"));
            byte[] response = body.get().getBytes(StandardCharsets.UTF_8);
            request.getResponseHeaders().set("Content-Type", "application/json");
            request.sendResponseHeaders(status.get(), response.length);
            request.getResponseBody().write(response);
            request.close();
        });
        graph.start();
    }

    @AfterEach void stop() { graph.stop(0); }

    @Test void checksAuthorityBeforeEveryRequestAndReturnsOnlyTheBoundedProfile() throws Exception {
        var token = new AtomicReference<>("initial");
        var connected = new AtomicBoolean(true);
        var capability = clients().create(access(token, connected));

        var profile = capability.me();
        assertEquals("account-1", profile.id());
        assertEquals("Ada Lovelace", profile.displayName().orElseThrow());
        assertEquals("ada@example.test", profile.userPrincipalName().orElseThrow());
        assertTrue(profile.mail().isEmpty());
        assertEquals("Bearer initial", authorization.get());

        token.set("renewed-by-quarkus");
        capability.me();
        assertEquals("Bearer renewed-by-quarkus", authorization.get());

        connected.set(false);
        assertThrows(ConnectionFailure.class, capability::me);
        assertEquals(2, calls.get());
    }

    @Test void authenticationChallengeRequiresInteractionWithoutRetrying() {
        var connected = new AtomicBoolean(true);
        var capability = clients().create(access(new AtomicReference<>("rejected"), connected));
        status.set(401);
        body.set("{\"error\":{\"code\":\"InvalidAuthenticationToken\"}}");

        var failure = assertThrows(ConnectionFailure.class, capability::me);
        assertEquals(ConnectionFailure.Reason.REAUTHORIZE, failure.reason());
        assertFalse(connected.get());
        assertEquals(1, calls.get());
    }

    @Test void otherFailuresAndInvalidProfilesDoNotExposeProviderBodies() {
        var capability = clients().create(access(new AtomicReference<>("current"), new AtomicBoolean(true)));
        status.set(403);
        body.set("super-secret-provider-detail");
        var denied = assertThrows(MicrosoftGraphRequestException.class, capability::me);
        assertEquals(403, denied.statusCode());
        assertFalse(denied.getMessage().contains(body.get()));

        status.set(200);
        body.set("{\"displayName\":\"Missing identity\"}");
        var invalid = assertThrows(MicrosoftGraphRequestException.class, capability::me);
        assertEquals(200, invalid.statusCode());
    }

    @Test void rejectsRedirectingClientsAndNonOriginEndpoints() {
        var redirects = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build();
        assertThrows(IllegalArgumentException.class,
            () -> new MicrosoftGraphClients(redirects, origin(), new ObjectMapper(), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> new MicrosoftGraphClients(HttpClient.newHttpClient(), URI.create("https://graph.microsoft.com/v1.0/me"),
                new ObjectMapper(), Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
            () -> new MicrosoftGraphClients(HttpClient.newHttpClient(), URI.create("urn:microsoft:graph"),
                new ObjectMapper(), Duration.ofSeconds(1)));
        redirects.close();
    }

    private MicrosoftGraphClients clients() {
        return new MicrosoftGraphClients(HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build(),
            origin(), new ObjectMapper(), Duration.ofSeconds(2));
    }

    private URI origin() { return URI.create("http://127.0.0.1:" + graph.getAddress().getPort()); }

    private ConnectionRegistration.RequestAccess access(AtomicReference<String> token, AtomicBoolean connected) {
        return new ConnectionRegistration.RequestAccess() {
            @Override public String accessToken() {
                if (!connected.get()) { throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE); }
                return token.get();
            }
            @Override public void requiresInteraction() { connected.set(false); }
        };
    }
}
