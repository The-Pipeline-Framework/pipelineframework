package org.pipelineframework.csv.orchestrator.service;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
}
