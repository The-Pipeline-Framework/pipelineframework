package org.pipelineframework.query;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
        Files.writeString(explicit, pipelineYaml("v2"));
        System.setProperty("pipeline.config", relativeToWorkingDirectory(explicit).toString());

        QueryStepDescriptorFactory factory = new QueryStepDescriptorFactory();
        try {
            QueryStepDescriptor descriptor = factory.descriptor(
                "LoadCustomerRisk",
                "org.example.CustomerRiskLookup",
                "org.example.CustomerRiskSnapshot").await().atMost(Duration.ofSeconds(2));

            assertEquals("customer-risk-by-id", descriptor.queryId());
            assertEquals("jpa", descriptor.connector());
            assertEquals("v2", descriptor.version());
            assertEquals("org.example.CustomerRiskEntity", descriptor.jpa().entity());
            assertEquals("eq", descriptor.jpa().where().get("customerId").operator());
            assertEquals(List.of("input.customerId"), descriptor.jpa().where().get("customerId").values());
        } finally {
            factory.shutdown();
        }
    }

    @Test
    void descriptorResolvesExplicitConfigDirectory() throws Exception {
        Files.writeString(tempDir.resolve("pipeline.yaml"), pipelineYaml("v1"));
        System.setProperty("pipeline.config", tempDir.toString());

        QueryStepDescriptorFactory factory = new QueryStepDescriptorFactory();
        try {
            QueryStepDescriptor descriptor = factory.descriptor(
                "ProcessLoadCustomerRiskService",
                "org.example.CustomerRiskLookup",
                "org.example.CustomerRiskSnapshot").await().atMost(Duration.ofSeconds(2));

            assertEquals("jpa", descriptor.connector());
        } finally {
            factory.shutdown();
        }
    }

    private static Path relativeToWorkingDirectory(Path path) {
        return Path.of("").toAbsolutePath().normalize().relativize(path.toAbsolutePath().normalize());
    }

    private static String pipelineYaml(String version) {
        return """
            basePackage: org.example
            transport: GRPC
            queries:
              customer-risk-by-id:
                connector: jpa
                input: org.example.CustomerRiskLookup
                output: org.example.CustomerRiskSnapshot
                version: %s
                jpa:
                  entity: org.example.CustomerRiskEntity
                  where:
                    customerId: input.customerId
                  projection:
                    customerId: customerId
                    riskBand: riskBand
                  result: single
            steps:
              - name: Load Customer Risk
                kind: query
                cardinality: ONE_TO_ONE
                query: customer-risk-by-id
                input: org.example.CustomerRiskLookup
                output: org.example.CustomerRiskSnapshot
                capture:
                  keyFields: [customerId]
            """.formatted(version);
    }
}
