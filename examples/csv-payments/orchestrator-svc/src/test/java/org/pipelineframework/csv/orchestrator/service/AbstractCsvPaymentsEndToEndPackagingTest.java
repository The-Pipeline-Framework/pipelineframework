package org.pipelineframework.csv.orchestrator.service;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractCsvPaymentsEndToEndPackagingTest {

    @TempDir
    Path tempDir;

    @Test
    void runtimeMappingsMatchReturnsFalseWhenActiveMappingIsMissing() throws Exception {
        Path active = tempDir.resolve("pipeline.runtime.yaml");
        Path desired = tempDir.resolve("modular-strict.yaml");
        Files.writeString(desired, "runtime:\n  layout: modular\n");

        assertFalse(AbstractCsvPaymentsEndToEnd.runtimeMappingsMatch(active, desired));
    }

    @Test
    void runtimeMappingsMatchReturnsTrueForSameMappingContent() throws Exception {
        Path active = tempDir.resolve("pipeline.runtime.yaml");
        Path desired = tempDir.resolve("modular-strict.yaml");
        String mapping = "runtime:\n  layout: modular\n";
        Files.writeString(active, mapping);
        Files.writeString(desired, mapping);

        assertTrue(AbstractCsvPaymentsEndToEnd.runtimeMappingsMatch(active, desired));
    }

    @Test
    void runtimeMappingsMatchReturnsFalseWhenContentDiffers() throws Exception {
        Path active = tempDir.resolve("pipeline.runtime.yaml");
        Path desired = tempDir.resolve("modular-strict.yaml");
        Files.writeString(active, "runtime:\n  layout: monolith\n");
        Files.writeString(desired, "runtime:\n  layout: modular\n");

        assertFalse(AbstractCsvPaymentsEndToEnd.runtimeMappingsMatch(active, desired));
    }

    @Test
    void packagedV3PersistenceProfileMustMatchTheRequestedProfile() throws Exception {
        Path jar = tempDir.resolve("quarkus-run.jar");
        Path marker = AbstractCsvPaymentsEndToEnd.packagedV3PersistenceProfileMarker(jar);
        String original = System.getProperty("csv.v3.persistence");
        try {
            System.setProperty("csv.v3.persistence", "true");
            assertFalse(AbstractCsvPaymentsEndToEnd.packagedV3PersistenceProfileMatches(jar));
            Files.writeString(marker, "true");
            assertTrue(AbstractCsvPaymentsEndToEnd.packagedV3PersistenceProfileMatches(jar));
            System.setProperty("csv.v3.persistence", "false");
            assertFalse(AbstractCsvPaymentsEndToEnd.packagedV3PersistenceProfileMatches(jar));
        } finally {
            if (original == null) {
                System.clearProperty("csv.v3.persistence");
            } else {
                System.setProperty("csv.v3.persistence", original);
            }
        }
    }
}
