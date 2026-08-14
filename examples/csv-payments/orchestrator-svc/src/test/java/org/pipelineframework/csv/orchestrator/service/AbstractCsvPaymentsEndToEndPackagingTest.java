package org.pipelineframework.csv.orchestrator.service;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    @Test
    void frameworkRuntimeContentHashIgnoresJarEntryTimestamps() throws Exception {
        Path first = writeJar("first.jar", 1_000L, "same runtime content");
        Path second = writeJar("second.jar", 2_000L, "same runtime content");
        Path changed = writeJar("changed.jar", 2_000L, "changed runtime content");

        String firstContentHash = AbstractCsvPaymentsEndToEnd.jarContentSha256(first);
        String secondContentHash = AbstractCsvPaymentsEndToEnd.jarContentSha256(second);

        assertNotEquals(-1L, Files.mismatch(first, second), "Fixture JAR archives must differ");
        assertEquals(firstContentHash, secondContentHash);
        assertNotEquals(firstContentHash, AbstractCsvPaymentsEndToEnd.jarContentSha256(changed));
        assertEquals(firstContentHash, hashJarWithProofScript(first));
        assertEquals(secondContentHash, hashJarWithProofScript(second));
    }

    private Path writeJar(String name, long timestamp, String content) throws Exception {
        Path jar = tempDir.resolve(name);
        try (OutputStream output = Files.newOutputStream(jar); JarOutputStream jarOutput = new JarOutputStream(output)) {
            JarEntry entry = new JarEntry("telemetry/contract.txt");
            entry.setTime(timestamp);
            jarOutput.putNextEntry(entry);
            jarOutput.write(content.getBytes(StandardCharsets.UTF_8));
            jarOutput.closeEntry();
        }
        return jar;
    }

    private String hashJarWithProofScript(Path jar) throws Exception {
        Path script = Path.of(System.getProperty("user.dir"))
                .resolve("../hash-jar-content.sh")
                .normalize();
        Process process = new ProcessBuilder("bash", script.toString(), jar.toString())
                .redirectErrorStream(true)
                .start();
        String output = new String(process.getInputStream().readAllBytes());
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));
        assertEquals(0, process.exitValue(), output);
        return output.strip();
    }

}
