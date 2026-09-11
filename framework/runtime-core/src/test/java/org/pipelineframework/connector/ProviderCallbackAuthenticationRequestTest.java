package org.pipelineframework.connector;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProviderCallbackAuthenticationRequestTest {
    private final ProviderCallbackRequest identity = new ProviderCallbackRequest("tenant-secret", "interaction-secret",
        new ConnectorOperationIdentity(ConnectorProviderId.of("http.client"), "job.start", ConnectorOperationKind.COMMAND, 1),
        "job.completed");

    @Test
    void copiesSensitiveMaterialAndRedactsDiagnostics() {
        var security = new ProviderCallbackAuthenticationRequest.SecurityRequirement("callbackSignature", List.of(),
            List.of(new ProviderCallbackAuthenticationRequest.SecurityTarget("HEADER", "X-Signature")));
        assertEquals("callbackSignature", security.scheme());
        byte[] body = {1, 2};
        var request = new ProviderCallbackAuthenticationRequest(identity, "POST", Map.of("Signature", List.of("secret")),
            body, List.of());
        body[0] = 9;
        request.body()[1] = 9;
        assertArrayEquals(new byte[]{1, 2}, request.body());
        assertEquals(List.of("secret"), request.headers().get("signature"));
        assertThrows(UnsupportedOperationException.class, () -> request.headers().clear());
        assertFalse(request.toString().contains("secret"));
        assertFalse(identity.toString().contains("tenant-secret"));
        assertFalse(identity.toString().contains("interaction-secret"));
    }

    @Test
    void rejectsUnboundedAndAmbiguousHeadersAndUnsafeActorIdentity() {
        assertThrows(IllegalArgumentException.class, () -> ProviderCallbackAuthenticationRequest.boundedHeaders(
            Map.of("Signature", List.of("a"), "signature", List.of("b"))));
        assertThrows(IllegalArgumentException.class, () -> ProviderCallbackAuthenticationRequest.boundedHeaders(
            Map.of("Signature", List.of("a\r\nb"))));
        assertThrows(IllegalArgumentException.class, () -> ProviderCallbackAuthenticationRequest.boundedHeaders(
            Map.of("Signature", List.of("x".repeat(8193)))));
        assertThrows(IllegalArgumentException.class, () -> ProviderCallbackAuthenticationRequest.boundedHeaders(
            Map.of("Signature", List.of("€".repeat(4000), "€".repeat(4000), "€".repeat(4000)))));
        assertThrows(IllegalArgumentException.class, () -> new ProviderCallbackActor("raw bearer secret"));
        assertThrows(IllegalArgumentException.class, () -> new ProviderCallbackActor("x".repeat(129)));
    }
}
