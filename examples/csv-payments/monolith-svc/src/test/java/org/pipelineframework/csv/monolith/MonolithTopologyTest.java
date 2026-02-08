package org.pipelineframework.csv.monolith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.InputStream;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class MonolithTopologyTest {

    @Test
    void routesAllGeneratedGrpcClientsToSingleRuntimeEndpoint() throws Exception {
        String transportOverride = System.getProperty("pipeline.transport");
        if (transportOverride == null || transportOverride.isBlank()) {
            transportOverride = System.getenv("PIPELINE_TRANSPORT");
        }
        boolean localTransport = transportOverride != null && "LOCAL".equalsIgnoreCase(transportOverride.trim());
        try (InputStream input = Thread.currentThread()
            .getContextClassLoader()
            .getResourceAsStream("META-INF/pipeline/orchestrator-clients.properties")) {
            if (localTransport) {
                assertNull(input, "Local transport should not emit orchestrator client metadata.");
                return;
            }
            assertNotNull(input, "Expected generated orchestrator client metadata");
            Properties properties = new Properties();
            properties.load(input);

            Set<String> hostKeys = properties.stringPropertyNames().stream()
                .filter(name -> name.startsWith("quarkus.grpc.clients.") && name.endsWith(".host"))
                .collect(Collectors.toSet());
            Set<String> portKeys = properties.stringPropertyNames().stream()
                .filter(name -> name.startsWith("quarkus.grpc.clients.") && name.endsWith(".port"))
                .collect(Collectors.toSet());

            assertFalse(hostKeys.isEmpty(), "No gRPC client host entries found");
            String prefix = "quarkus.grpc.clients.";
            Set<String> hostClientIds = hostKeys.stream()
                .map(key -> key.substring(prefix.length(), key.length() - ".host".length()))
                .collect(Collectors.toSet());
            Set<String> portClientIds = portKeys.stream()
                .map(key -> key.substring(prefix.length(), key.length() - ".port".length()))
                .collect(Collectors.toSet());

            assertEquals(hostClientIds, portClientIds, "Expected matching host/port client entries");
            hostClientIds.forEach(clientId -> {
                assertEquals("localhost", properties.getProperty(prefix + clientId + ".host"));
                assertEquals("8444", properties.getProperty(prefix + clientId + ".port"));
            });
        }
    }
}
