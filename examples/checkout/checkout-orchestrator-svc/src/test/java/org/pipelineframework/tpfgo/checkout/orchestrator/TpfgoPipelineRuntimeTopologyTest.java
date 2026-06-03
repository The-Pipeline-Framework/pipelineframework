package org.pipelineframework.tpfgo.checkout.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import static org.junit.jupiter.api.Assertions.fail;

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

            assertEquals(Set.of("${PIPELINE_RUNTIME_HOST:127.0.0.1}"), hosts,
                "Grouped runtime clients should preserve the externalized runtime host");
            assertEquals(Set.of("${PIPELINE_RUNTIME_GRPC_PORT:9000}"), ports,
                "Grouped runtime clients should preserve the externalized runtime port");
            assertFalse(properties.stringPropertyNames().stream().anyMatch(name -> name.contains("tpfgo.checkout.order-pending.v1")),
                "Checkpoint publication bindings should not appear in orchestrator client metadata");
            assertRuntimeMappingActive();
            assertPipelineRuntimePortMatchesClientPort();
        }
    }

    private static void assertRuntimeMappingActive() throws IOException {
        Path mappingPath = resolveRuntimeMappingPath();
        String content = Files.readString(mappingPath);
        if (!content.contains("layout: pipeline-runtime")) {
            fail("Expected active runtime mapping to use layout: pipeline-runtime");
        }
    }

    private static Path resolveRuntimeMappingPath() {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("config/pipeline.runtime.yaml");
        if (resource != null) {
            try {
                return Path.of(resource.toURI()).normalize();
            } catch (URISyntaxException e) {
                fail("Invalid runtime mapping resource URI: " + resource, e);
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
        fail("Could not resolve examples/checkout/config/pipeline.runtime.yaml");
        return Path.of(".");
    }

    private static void assertPipelineRuntimePortMatchesClientPort() throws IOException {
        Path configPath = resolveCheckoutRoot()
            .resolve("pipeline-runtime-svc")
            .resolve("src")
            .resolve("main")
            .resolve("resources")
            .resolve("application.properties");
        String content = Files.readString(configPath);
        assertTrue(content.contains("quarkus.http.port=${PIPELINE_RUNTIME_GRPC_PORT:9000}"),
            "pipeline-runtime-svc should listen on the same externalized port used by generated clients");
        assertTrue(content.contains("quarkus.grpc.server.use-separate-server=false"),
            "pipeline-runtime-svc should keep gRPC bound to the HTTP listener port");
    }

    private static Path resolveCheckoutRoot() {
        Path userDir = Path.of(System.getProperty("user.dir", ".")).normalize();
        if (Files.exists(userDir.resolve("pipeline-runtime-svc"))) {
            return userDir;
        }
        Path parent = userDir.resolve("..").normalize();
        if (Files.exists(parent.resolve("pipeline-runtime-svc"))) {
            return parent;
        }
        fail("Could not resolve examples/checkout project root");
        return Path.of(".");
    }
}
