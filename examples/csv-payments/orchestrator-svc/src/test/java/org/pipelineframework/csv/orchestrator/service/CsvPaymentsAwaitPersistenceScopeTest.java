package org.pipelineframework.csv.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class CsvPaymentsAwaitPersistenceScopeTest {

    @Test
    void persistenceAspectTargetsConcreteServiceSteps() throws IOException {
        String pipelineYaml = Files.readString(resolveConfigPath("pipeline.yaml"));

        assertTrue(pipelineYaml.contains("aspects:\n  persistence:"), "Expected persistence aspect in pipeline.yaml");
        assertTrue(pipelineYaml.contains("scope: STEPS"), "Persistence should be scoped to selected steps");
        assertTrue(
                pipelineYaml.contains("- ProcessFolderService"),
                "Persistence should include the folder ingestion step");
        assertTrue(
                pipelineYaml.contains("- ProcessCsvPaymentsInputService"),
                "Persistence should include the CSV parsing step");
        assertTrue(
                pipelineYaml.contains("- ProcessPaymentStatusService"),
                "Persistence should include post-await status processing with idempotent entity keys");
        assertTrue(
                pipelineYaml.contains("- ProcessCsvPaymentsOutputFileService"),
                "Persistence should include output generation with idempotent entity keys");
        assertFalse(
                pipelineYaml.contains("- Await Payment Provider"),
                "Persistence should not target the replayable await boundary");
    }

    @Test
    void csvRuntimeLayoutsDoNotRelyOnDuplicateKeyUpsert() throws IOException {
        List<String> configFiles = List.of(
                "persistence-svc/src/main/resources/application.properties",
                "pipeline-runtime-svc/src/main/resources/application.properties",
                "monolith-svc/src/main/resources/application.properties");

        for (String relativePath : configFiles) {
            String config = Files.readString(resolveExamplePath(relativePath));
            assertTrue(
                    config.contains("persistence.duplicate-key=ignore"),
                    () -> relativePath + " should ignore duplicate idempotent persistence side effects");
        }
    }

    @Test
    void awaitRequestTopicMatchesPaymentProviderRequestTopic() throws IOException {
        Properties orchestratorConfig = loadExampleProperties("orchestrator-svc/src/main/resources/application.properties");
        Properties providerConfig = loadExampleProperties("payments-processing-svc/src/main/resources/application.properties");
        Properties monolithConfig = loadExampleProperties("monolith-svc/src/main/resources/application.properties");

        String providerRequestTopic = providerConfig.getProperty("mp.messaging.incoming.csv-payment-provider-requests.topic");
        assertEquals(
                providerRequestTopic,
                orchestratorConfig.getProperty("mp.messaging.outgoing.tpf-await-kafka-requests.topic"),
                "Modular orchestrator await requests must be published to the provider request topic");
        assertEquals(
                monolithConfig.getProperty("mp.messaging.incoming.csv-payment-provider-requests.topic"),
                monolithConfig.getProperty("mp.messaging.outgoing.tpf-await-kafka-requests.topic"),
                "Monolith await requests must be published to the in-process provider request topic");
    }

    private static Path resolveConfigPath(String fileName) {
        URL resource = Thread.currentThread().getContextClassLoader().getResource("config/" + fileName);
        if (resource != null) {
            try {
                return Path.of(resource.toURI()).normalize();
            } catch (URISyntaxException e) {
                throw new IllegalStateException("Invalid config resource URI: " + resource, e);
            }
        }

        Path userDir = Path.of(System.getProperty("user.dir", ".")).normalize();
        Path direct = userDir.resolve("config").resolve(fileName).normalize();
        if (Files.exists(direct)) {
            return direct;
        }

        Path parent = userDir.resolve("..").resolve("config").resolve(fileName).normalize();
        if (Files.exists(parent)) {
            return parent;
        }

        throw new IllegalStateException("Could not resolve config/" + fileName);
    }

    private static Path resolveExamplePath(String relativePath) {
        Path userDir = Path.of(System.getProperty("user.dir", ".")).normalize();
        Path direct = userDir.resolve(relativePath).normalize();
        if (Files.exists(direct)) {
            return direct;
        }

        Path parent = userDir.resolve("..").resolve(relativePath).normalize();
        if (Files.exists(parent)) {
            return parent;
        }

        throw new IllegalStateException("Could not resolve " + relativePath);
    }

    private static Properties loadExampleProperties(String relativePath) throws IOException {
        Properties properties = new Properties();
        properties.load(new StringReader(Files.readString(resolveExamplePath(relativePath))));
        return properties;
    }
}
