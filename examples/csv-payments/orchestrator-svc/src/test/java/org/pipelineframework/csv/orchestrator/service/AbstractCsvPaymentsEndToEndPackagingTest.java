package org.pipelineframework.csv.orchestrator.service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void frameworkVersionResolutionIgnoresInheritedMavenVersionOutput() throws Exception {
        Path fakeMaven = tempDir.resolve("mvnw");
        Files.writeString(fakeMaven, """
                #!/usr/bin/env bash
                set -euo pipefail
                if [[ -n "${MAVEN_ARGS:-}" ]]; then
                  echo "MAVEN_ARGS leaked into project-version evaluation" >&2
                  exit 42
                fi
                printf '27.7.2-SNAPSHOT\\n'
                """);
        assertTrue(fakeMaven.toFile().setExecutable(true));

        Path resolver = Path.of(System.getProperty("user.dir"))
                .resolve("../resolve-framework-version.sh")
                .normalize();
        ProcessBuilder processBuilder = new ProcessBuilder(
                        "bash",
                        resolver.toString(),
                        fakeMaven.toString(),
                        tempDir.resolve("pom.xml").toString(),
                        tempDir.resolve("repository").toString())
                .redirectErrorStream(true);
        processBuilder.environment().put("MAVEN_ARGS", "-B -V --no-transfer-progress");
        Process process = processBuilder.start();
        process.getOutputStream().close();
        String output = new String(process.getInputStream().readAllBytes());

        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue(), output);
        assertEquals("27.7.2-SNAPSHOT", output.strip());
    }

}
