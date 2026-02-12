package org.pipelineframework.csv.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class PipelineRuntimeTopologyTest {
    private static final String OBSERVE_PERSISTENCE_PREFIX = "observe-persistence-";

    @Test
    void routesPipelineAndPluginClientsToTwoGroupedRuntimeEndpoints() throws Exception {
        String layout = System.getProperty("csv.runtime.layout", System.getenv("CSV_RUNTIME_LAYOUT"));
        assumeTrue(layout != null && "pipeline-runtime".equalsIgnoreCase(layout.trim()),
                "Test applies only to pipeline-runtime layout.");
        assumeTrue(isPipelineRuntimeMappingActive(),
                "Test requires active examples/csv-payments/config/pipeline.runtime.yaml with layout: pipeline-runtime"
                        + " (use examples/csv-payments/build-pipeline-runtime.sh).");

        try (InputStream input = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream("META-INF/pipeline/orchestrator-clients.properties")) {
            assertNotNull(input, "Expected generated orchestrator client metadata");
            Properties properties = new Properties();
            properties.load(input);

            String prefix = "quarkus.grpc.clients.";
            Set<String> hostKeys = properties.stringPropertyNames().stream()
                    .filter(name -> name.startsWith(prefix) && name.endsWith(".host"))
                    .collect(Collectors.toSet());
            Set<String> portKeys = properties.stringPropertyNames().stream()
                    .filter(name -> name.startsWith(prefix) && name.endsWith(".port"))
                    .collect(Collectors.toSet());

            assertFalse(hostKeys.isEmpty(), "No gRPC client host entries found");
            assertFalse(portKeys.isEmpty(), "No gRPC client port entries found");
            Set<String> hostClientIds = hostKeys.stream()
                    .map(key -> key.substring(prefix.length(), key.length() - ".host".length()))
                    .collect(Collectors.toSet());
            Set<String> portClientIds = portKeys.stream()
                    .map(key -> key.substring(prefix.length(), key.length() - ".port".length()))
                    .collect(Collectors.toSet());
            assertEquals(hostClientIds, portClientIds, "Expected matching host/port client entries");

            Set<String> pipelineClientIds = hostClientIds.stream()
                    .filter(clientId -> !clientId.startsWith(OBSERVE_PERSISTENCE_PREFIX))
                    .collect(Collectors.toSet());
            Set<String> pluginClientIds = hostClientIds.stream()
                    .filter(clientId -> clientId.startsWith(OBSERVE_PERSISTENCE_PREFIX))
                    .collect(Collectors.toSet());

            assertFalse(pipelineClientIds.isEmpty(), "No grouped pipeline-runtime clients found");
            assertFalse(pluginClientIds.isEmpty(), "No grouped plugin-runtime clients found");

            Set<String> pipelineHosts = pipelineClientIds.stream()
                    .map(clientId -> properties.getProperty(prefix + clientId + ".host"))
                    .collect(Collectors.toSet());
            Set<String> pluginHosts = pluginClientIds.stream()
                    .map(clientId -> properties.getProperty(prefix + clientId + ".host"))
                    .collect(Collectors.toSet());
            Set<String> pipelinePorts = pipelineClientIds.stream()
                    .map(clientId -> properties.getProperty(prefix + clientId + ".port"))
                    .collect(Collectors.toSet());
            Set<String> pluginPorts = pluginClientIds.stream()
                    .map(clientId -> properties.getProperty(prefix + clientId + ".port"))
                    .collect(Collectors.toSet());

            assertEquals(Set.of("localhost"), pipelineHosts, "Pipeline-runtime clients should target localhost");
            assertEquals(Set.of("localhost"), pluginHosts, "Plugin-runtime clients should target localhost");
            assertEquals(1, pipelinePorts.size(), "Pipeline-runtime clients should share one port");
            assertEquals(1, pluginPorts.size(), "Plugin-runtime clients should share one port");
            assertNotEquals(pipelinePorts, pluginPorts,
                    "Pipeline-runtime and plugin-runtime clients should not share the same endpoint");
        }
    }

    private static boolean isPipelineRuntimeMappingActive() {
        Path mappingPath = resolveRuntimeMappingPath();
        if (mappingPath == null || !Files.exists(mappingPath)) {
            return false;
        }
        try {
            String content = Files.readString(mappingPath);
            return content.contains("layout: pipeline-runtime");
        } catch (IOException ignored) {
            return false;
        }
    }

    private static Path resolveRuntimeMappingPath() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("config/pipeline.runtime.yaml");
        if (resource != null) {
            try {
                return Path.of(resource.toURI()).normalize();
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid runtime mapping resource URI: " + resource, e);
            }
        }

        Path userDir = Path.of(System.getProperty("user.dir", ".")).normalize();
        Path direct = userDir.resolve("config").resolve("pipeline.runtime.yaml").normalize();
        if (Files.exists(direct)) {
            return direct;
        }

        Path parent = userDir.resolve("..").resolve("config").resolve("pipeline.runtime.yaml").normalize();
        if (Files.exists(parent)) {
            return parent;
        }
        return null;
    }
}
