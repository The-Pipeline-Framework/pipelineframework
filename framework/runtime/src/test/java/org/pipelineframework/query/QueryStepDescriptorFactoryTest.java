package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class QueryStepDescriptorFactoryTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void clearPipelineConfigProperty() {
        System.clearProperty("pipeline.config");
    }

    @Test
    void descriptorAcceptsCompactGeneratedServiceNameAndExplicitRelativeConfigFile() throws Exception {
        Path explicit = tempDir.resolve("query-config.yaml");
        Files.writeString(explicit, pipelineYaml("customer-risk", "v2"));
        System.setProperty("pipeline.config", relativeToWorkingDirectory(explicit).toString());

        QueryStepDescriptorFactory factory = new QueryStepDescriptorFactory();
        try {
            QueryStepDescriptor descriptor = factory.descriptor(
                "LoadCustomerRisk",
                "org.example.CustomerRiskLookup",
                "org.example.CustomerRiskSnapshot").await().atMost(Duration.ofSeconds(2));

            assertEquals("customer-risk-by-id", descriptor.queryId());
            assertEquals("customer-risk", descriptor.connector());
            assertEquals("v2", descriptor.version());
            assertEquals("customer_risk", descriptor.config().get("table"));
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void descriptorResolvesExplicitConfigDirectory() throws Exception {
        Files.writeString(tempDir.resolve("pipeline.yaml"), pipelineYaml("directory-risk", "v1"));
        System.setProperty("pipeline.config", tempDir.toString());

        QueryStepDescriptorFactory factory = new QueryStepDescriptorFactory();
        try {
            QueryStepDescriptor descriptor = factory.descriptor(
                "ProcessLoadCustomerRiskService",
                "org.example.CustomerRiskLookup",
                "org.example.CustomerRiskSnapshot").await().atMost(Duration.ofSeconds(2));

            assertEquals("directory-risk", descriptor.connector());
        } finally {
            factory.shutdown();
        }
    }

    private static Path relativeToWorkingDirectory(Path path) {
        return Path.of("").toAbsolutePath().normalize().relativize(path.toAbsolutePath().normalize());
    }

    private static String pipelineYaml(String connector, String version) {
        return """
            basePackage: org.example
            transport: GRPC
            queries:
              customer-risk-by-id:
                connector: %s
                input: org.example.CustomerRiskLookup
                output: org.example.CustomerRiskSnapshot
                version: %s
                config:
                  table: customer_risk
            steps:
              - name: Load Customer Risk
                kind: query
                cardinality: ONE_TO_ONE
                query: customer-risk-by-id
                input: org.example.CustomerRiskLookup
                output: org.example.CustomerRiskSnapshot
                capture:
                  keyFields: [customerId]
            """.formatted(connector, version);
    }
}
