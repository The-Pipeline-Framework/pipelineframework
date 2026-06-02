package org.pipelineframework.csv.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
        assertFalse(
                pipelineYaml.contains("- ProcessPaymentStatusService"),
                "Persistence should stay pre-await until post-await side effects have once-only checkpointing");
        assertFalse(
                pipelineYaml.contains("- ProcessCsvPaymentsOutputFileService"),
                "Persistence should stay pre-await until post-await side effects have once-only checkpointing");
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
            assertFalse(
                    config.contains("persistence.duplicate-key=upsert"),
                    () -> relativePath + " should not hide replay duplicates behind upsert");
        }
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
}
