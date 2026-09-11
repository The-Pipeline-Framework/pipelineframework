package org.pipelineframework.connector;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ConnectorCallbackContractTest {
    private static final String CALLBACK = """
        {"id":"job.completed","typeContract":{"input":"JobCompleted"},"required":true}
        """;

    @Test
    void schemaSevenRoundTripsCallbackAndLegacySchemasRemainReadable() {
        var manifest = read(7, ",\"callbacks\":[" + CALLBACK + "]");
        var callback = manifest.providers().getFirst().operations().getFirst().callbacks().getFirst();
        assertEquals("job.completed", callback.id());
        assertEquals("JobCompleted", callback.typeContract().inputType());
        assertTrue(callback.required());
        assertEquals(manifest, ConnectorProviderManifestReader.read(new ByteArrayInputStream(
            ConnectorProviderArtifacts.json(manifest).getBytes(StandardCharsets.UTF_8))));
        for (int schema = 1; schema <= 6; schema++) {
            assertTrue(read(schema, "").providers().getFirst().operations().getFirst().callbacks().isEmpty());
            int version = schema;
            assertThrows(IllegalArgumentException.class, () -> read(version, ",\"callbacks\":[]"));
        }
    }

    @Test
    void rejectsDuplicateIdsAndOutputContracts() {
        assertThrows(IllegalArgumentException.class, () -> read(7, ",\"callbacks\":[" + CALLBACK + "," + CALLBACK + "]"));
        assertThrows(IllegalArgumentException.class, () -> new ConnectorOperationCallbackDescriptor(
            "job.completed", new ConnectorOperationTypeContract("JobCompleted", Optional.of("Output")), true));
        var callback = new ConnectorOperationCallbackDescriptor("job.completed",
            new ConnectorOperationTypeContract("JobCompleted", Optional.empty()), true);
        assertThrows(IllegalArgumentException.class, () -> new ConnectorOperationDescriptor("find",
            ConnectorOperationKind.QUERY, 1, Optional.empty(), Optional.empty(), Optional.empty(),
            Optional.of(QueryOperationCardinality.ONE_TO_ONE), Optional.empty(), List.of(callback)));
    }

    @Test
    void runtimeCallbacksMustMatchTheManifestExactly() {
        var callback = new ConnectorOperationCallbackDescriptor("completed",
            new ConnectorOperationTypeContract("Completed", Optional.empty()), true);
        var expected = ConnectorDescriptors.operation(new CallbackOperation(List.of(callback)));
        assertEquals(expected, ConnectorDescriptors.operation(new CallbackOperation(List.of(callback)), expected));
        var changed = new ConnectorOperationCallbackDescriptor("completed",
            new ConnectorOperationTypeContract("DifferentPayload", Optional.empty()), true);
        var extra = new ConnectorOperationCallbackDescriptor("extra",
            new ConnectorOperationTypeContract("Completed", Optional.empty()), false);
        for (var callbacks : List.<List<ConnectorOperationCallbackDescriptor>>of(
            List.of(), List.of(changed), List.of(callback, extra))) {
            assertThrows(IllegalStateException.class,
                () -> ConnectorDescriptors.operation(new CallbackOperation(callbacks), expected));
        }
    }

    private record CallbackOperation(List<ConnectorOperationCallbackDescriptor> callbacks)
        implements CommandOperation<String, Void, String> {
        @Override public String id() { return "start"; }
        @Override public java.util.concurrent.CompletionStage<CommandOutcome<String>> dispatch(CommandInvocation<String, Void> invocation) {
            throw new UnsupportedOperationException("Descriptor-only fixture");
        }
    }

    @Test
    void callbackAuthorityRequiresSecureEndpointAndRedactsTokens() {
        var callback = new ConnectorCallbackContext("job.completed", URI.create("https://example.test/callback?token=secret"));
        assertFalse(callback.toString().contains("secret"));
        assertThrows(IllegalArgumentException.class, () -> new ConnectorCallbackContext("job.completed", URI.create("http://localhost/callback")));
        assertDoesNotThrow(() -> new ConnectorCallbackContext("job.completed", URI.create("http://localhost/callback"),
            ConnectorCallbackContext.UriPolicy.LOCAL_HTTP));
        assertThrows(IllegalArgumentException.class, () -> new ConnectorCallbackContext("job.completed",
            URI.create("http://host.docker.internal/callback")));
        assertDoesNotThrow(() -> new ConnectorCallbackContext("job.completed",
            URI.create("http://host.docker.internal/callback"), ConnectorCallbackContext.UriPolicy.LOCAL_HTTP));
        for (String uri : List.of("/relative", "https://user:password@example.test/callback", "https://example.test/callback#secret")) {
            assertThrows(IllegalArgumentException.class, () -> new ConnectorCallbackContext("job.completed", URI.create(uri)));
        }
    }

    private ConnectorProviderManifest read(int schema, String fields) {
        String json = "{\"schemaVersion\":" + schema + ",\"providers\":[{\"id\":\"test.provider\","
            + "\"version\":{\"major\":1,\"minor\":0},\"operations\":[{\"id\":\"start\","
            + "\"kind\":\"tpf:command\",\"majorVersion\":1" + fields + "}]}]}";
        return ConnectorProviderManifestReader.read(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
    }
}
