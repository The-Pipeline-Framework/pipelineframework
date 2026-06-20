package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;

class FilesystemObjectTargetProviderTest {

    @TempDir
    Path tempDir;

    @Test
    void writesObjectUnderConfiguredRoot() throws Exception {
        PipelineObjectPublishConfig target = target(tempDir);
        ObjectWriteRequest request = request(target, "results/payments.csv", "id,amount\n1,10\n");

        ObjectWriteResult result = new FilesystemObjectTargetProvider().write(request).await().indefinitely();

        assertEquals("id,amount\n1,10\n", Files.readString(tempDir.resolve("results/payments.csv")));
        assertEquals("filesystem", result.reference().provider());
        assertEquals(tempDir.toString(), result.reference().container());
        assertEquals("results/payments.csv", result.reference().key());
        assertEquals("text/csv", result.reference().contentType());
        assertEquals(request.checksum(), result.checksum());
        assertEquals(request.bytes().length, result.bytes());
    }

    @Test
    void rejectsEscapedOutputPath() {
        PipelineObjectPublishConfig target = target(tempDir);
        ObjectWriteRequest request = request(target, "../outside.csv", "bad");

        assertThrows(SecurityException.class, () ->
            new FilesystemObjectTargetProvider().write(request).await().indefinitely());
    }

    private PipelineObjectPublishConfig target(Path root) {
        return new PipelineObjectPublishConfig(
            "results",
            "object",
            "filesystem",
            Map.of("root", root.toString()),
            null,
            null);
    }

    private ObjectWriteRequest request(PipelineObjectPublishConfig target, String key, String body) {
        return new ObjectWriteRequest(
            target.name(),
            target,
            key,
            body.getBytes(StandardCharsets.UTF_8),
            "text/csv",
            Map.of("kind", "test"),
            "checksum",
            "idempotency");
    }
}
