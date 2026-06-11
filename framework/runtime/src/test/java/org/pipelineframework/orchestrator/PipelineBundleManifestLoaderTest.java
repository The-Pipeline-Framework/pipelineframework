package org.pipelineframework.orchestrator;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineBundleManifestLoaderTest {

    private final PipelineBundleManifestLoader loader = new PipelineBundleManifestLoader();

    @Test
    void readsValidManifest() {
        PipelineBundleManifest manifest = loader.load(stream("""
            {
              "schemaVersion": 1,
              "pipelineId": "org.example.orders",
              "bundleVersionId": "sha256:abc",
              "bundleHash": "abc",
              "platform": "COMPUTE",
              "transport": "REST",
              "module": "orchestrator-svc",
              "pluginHost": false,
              "runtimeLayout": "MONOLITH",
              "steps": [
                {
                  "index": 0,
                  "authoredName": "Validate",
                  "kind": "internal",
                  "cardinality": "ONE_TO_ONE",
                  "inputTypeId": "Order",
                  "outputTypeId": "ValidatedOrder",
                  "runtimeClass": "org.example.ValidateService",
                  "clientClass": "org.example.ValidateRestClientStep",
                  "awaitTransport": null
                }
              ],
              "capabilities": {
                "localTransitionExecution": true,
                "transitionWorkerProtocols": ["local", "rest", "grpc", "sqs"]
              }
            }
            """));

        assertEquals("org.example.orders", manifest.pipelineId());
        assertEquals("sha256:abc", manifest.bundleVersionId());
        assertEquals("MONOLITH", manifest.runtimeLayout());
        assertEquals("Validate", manifest.steps().getFirst().authoredName());
        assertTrue(manifest.capabilities().localTransitionExecution());
    }

    @Test
    void absentManifestReturnsEmpty() throws Exception {
        try (URLClassLoader classLoader = new URLClassLoader(new URL[0], null)) {
            assertFalse(loader.load(classLoader).isPresent());
        }
    }

    @Test
    void unsupportedSchemaFailsClearly() {
        IllegalStateException failure = assertThrows(IllegalStateException.class, () -> loader.load(stream("""
            {
              "schemaVersion": 2,
              "pipelineId": "org.example.orders",
              "bundleVersionId": "sha256:abc",
              "bundleHash": "abc"
            }
            """)));

        assertTrue(failure.getMessage().contains("Invalid META-INF/pipeline/bundle-manifest.json"));
    }

    private static ByteArrayInputStream stream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }
}
