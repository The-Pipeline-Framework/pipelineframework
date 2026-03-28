package org.pipelineframework.tpfgo.checkout.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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

class TpfgoPipelineRuntimeTopologyTest {

    @Test
    void routesInternalStepClientsToGroupedPipelineRuntime() throws Exception {
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
            assertEquals(hostKeys.size(), portKeys.size(), "Expected matching host/port client entries");

            Set<String> clientIds = hostKeys.stream()
                .map(key -> key.substring(prefix.length(), key.length() - ".host".length()))
                .collect(Collectors.toSet());

            assertEquals(Set.of("process-checkout-create-pending", "process-checkout-validate-request"), clientIds);
            Set<String> hosts = clientIds.stream()
                .map(clientId -> properties.getProperty(prefix + clientId + ".host"))
                .collect(Collectors.toSet());
            Set<String> ports = clientIds.stream()
                .map(clientId -> properties.getProperty(prefix + clientId + ".port"))
                .collect(Collectors.toSet());

            assertEquals(Set.of("127.0.0.1"), hosts, "Grouped runtime clients should target the shared runtime host");
            assertEquals(Set.of("9000"), ports, "Grouped runtime clients should share the configured runtime port");
            assertFalse(properties.stringPropertyNames().stream().anyMatch(name -> name.contains("tpfgo.checkout.order-pending.v1")),
                "Checkpoint publication bindings should not appear in orchestrator client metadata");
            assertRuntimeMappingActive();
        }
    }

    private static void assertRuntimeMappingActive() throws IOException {
        Path mappingPath = resolveRuntimeMappingPath();
        String content = Files.readString(mappingPath);
        if (!content.contains("layout: pipeline-runtime")) {
            throw new IllegalStateException("Expected active runtime mapping to use layout: pipeline-runtime");
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
        throw new IllegalStateException("Could not resolve examples/checkout/config/pipeline.runtime.yaml");
    }
}
