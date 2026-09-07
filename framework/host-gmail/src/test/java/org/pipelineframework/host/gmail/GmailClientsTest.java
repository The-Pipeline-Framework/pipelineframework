package org.pipelineframework.host.gmail;

import static org.junit.jupiter.api.Assertions.*;

import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pipelineframework.host.oidc.ConnectionFailure;
import org.pipelineframework.host.oidc.ConnectionRegistration;

class GmailClientsTest {
    @Test void checksAuthorityAtExecutionAndNeverOwnsRenewal() throws Exception {
        var current = new AtomicReference<>("initial");
        var connected = new AtomicBoolean(true);
        var calls = new AtomicInteger();
        var actual = new AtomicReference<>("");
        var transport = new MockHttpTransport() {
            @Override public LowLevelHttpRequest buildRequest(String method, String url) {
                assertEquals("GET", method);
                assertTrue(url.startsWith("https://gmail.googleapis.com/"));
                return new MockLowLevelHttpRequest(url) {
                    @Override public LowLevelHttpResponse execute() {
                        calls.incrementAndGet();
                        actual.set(getFirstHeaderValue("Authorization"));
                        return new MockLowLevelHttpResponse().setStatusCode(200).setContentType("application/json")
                            .setContent("{\"messages\":[{\"id\":\"message-1\"}]}");
                    }
                };
            }
        };
        var capability = new GmailClients(transport, GsonFactory.getDefaultInstance()).create(new ConnectionRegistration.RequestAccess() {
            @Override public String accessToken() {
                if (!connected.get()) { throw new ConnectionFailure(ConnectionFailure.Reason.REAUTHORIZE); }
                return current.get();
            }
            @Override public void requiresInteraction() { connected.set(false); }
        });
        assertEquals("message-1", capability.client().users().messages().list("me").execute().getMessages().getFirst().getId());
        assertEquals("Bearer initial", actual.get());
        current.set("renewed-by-quarkus");
        capability.client().users().messages().list("me").execute();
        assertEquals("Bearer renewed-by-quarkus", actual.get());
        var preparedBeforeDisconnect = capability.client().users().messages().list("me");
        connected.set(false);
        assertThrows(ConnectionFailure.class, preparedBeforeDisconnect::execute);
        assertEquals(2, calls.get());
    }
}
