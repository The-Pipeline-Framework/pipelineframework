package org.pipelineframework.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class RepositoryShellSupportTest {

    @Test
    void propagatesUnixAndTlsTcpDockerContextsForTestcontainers() throws Exception {
        assertShellTestPasses(".mvn/testcontainers-docker-context-test.sh");
    }

    @Test
    void fullVerifyLauncherPreventsDuplicatesAndReportsSuccess() throws Exception {
        assertShellTestPasses(".mvn/full-verify-launcher-test.sh");
    }

    private void assertShellTestPasses(String relativePath) throws Exception {
        Path testScript = repositoryRoot().resolve(relativePath);
        Process process = new ProcessBuilder("sh", testScript.toString())
            .redirectErrorStream(true)
            .start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);

        assertEquals(0, process.waitFor(), output);
    }

    private Path repositoryRoot() throws IOException {
        Path current = Path.of("").toAbsolutePath();
        while (current.getParent() != null) {
            if (Files.isRegularFile(current.resolve(".mvn/testcontainers-docker-context-test.sh"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IOException("Cannot locate repository root from " + Path.of("").toAbsolutePath());
    }
}
