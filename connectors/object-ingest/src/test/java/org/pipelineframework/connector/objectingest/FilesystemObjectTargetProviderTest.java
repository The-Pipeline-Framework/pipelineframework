package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;

class FilesystemObjectTargetProviderTest {

    @TempDir
    Path tempDir;

    @Test
    void streamsChunksAndMovesTempFileOnClose() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        ObjectWriteSession session = provider.open(openRequest(target(tempDir), "results/payments.csv"))
            .toCompletableFuture().join();

        session.write(ByteBuffer.wrap("id,amount\n".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.write(ByteBuffer.wrap("1,10\n".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        ObjectWriteResult result = session.close(new ObjectWriteCloseRequest(
            "id,amount\n1,10\n".getBytes(StandardCharsets.UTF_8).length,
            "checksum",
            Map.of("recordCount", "1"))).toCompletableFuture().join();

        assertEquals("id,amount\n1,10\n", Files.readString(tempDir.resolve("results/payments.csv")));
        assertEquals("filesystem", result.reference().provider());
        assertEquals(tempDir.toString(), result.reference().container());
        assertEquals("results/payments.csv", result.reference().key());
        assertEquals("text/csv", result.reference().contentType());
        assertEquals("checksum", result.checksum());
        assertEquals("1", result.reference().metadata().get("recordCount"));
        assertTrue(Files.list(tempDir.resolve("results"))
            .noneMatch(path -> path.getFileName().toString().contains(".tpf-publish-")));
    }

    @Test
    void abortDeletesTempFile() throws Exception {
        FilesystemObjectTargetProvider provider = new FilesystemObjectTargetProvider(Runnable::run);
        ObjectWriteSession session = provider.open(openRequest(target(tempDir), "results/payments.csv"))
            .toCompletableFuture().join();

        session.write(ByteBuffer.wrap("partial".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.abort(new RuntimeException("failed")).toCompletableFuture().join();

        assertFalse(Files.exists(tempDir.resolve("results/payments.csv")));
        assertTrue(Files.list(tempDir.resolve("results"))
            .noneMatch(path -> path.getFileName().toString().contains(".tpf-publish-")));
    }

    @Test
    void rejectsEscapedOutputPath() {
        PipelineObjectPublishConfig target = target(tempDir);

        CompletionException exception = assertThrows(CompletionException.class, () ->
            new FilesystemObjectTargetProvider(Runnable::run).open(openRequest(target, "../outside.csv"))
                .toCompletableFuture().join());
        assertTrue(exception.getCause() instanceof SecurityException);
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

    private ObjectWriteOpenRequest openRequest(PipelineObjectPublishConfig target, String key) {
        return new ObjectWriteOpenRequest(
            target.name(),
            target,
            key,
            "text/csv",
            Map.of("kind", "test"),
            "idempotency");
    }
}
