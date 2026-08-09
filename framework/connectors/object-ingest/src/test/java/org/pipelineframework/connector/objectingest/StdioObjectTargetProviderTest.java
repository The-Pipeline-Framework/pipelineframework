package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteSession;

class StdioObjectTargetProviderTest {

    @Test
    void writesFlushesAndNeverClosesStdout() {
        TrackingOutputStream output = new TrackingOutputStream();
        StdioObjectTargetProvider provider = provider(output);
        ObjectWriteSession session = provider.open(request(target(Map.of("endpoint", "stdout"))))
            .toCompletableFuture().join();

        session.write(ByteBuffer.wrap("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))).toCompletableFuture().join();
        session.close(new ObjectWriteCloseRequest(11L, "checksum", Map.of("records", "1"))).toCompletableFuture().join();

        assertEquals("{\"ok\":true}", output.toString(StandardCharsets.UTF_8));
        assertTrue(output.flushed);
        assertFalse(output.closed);
    }

    @Test
    void rejectsFilesystemStyleConfigurationAndExplainsNonAtomicAbort() {
        StdioObjectTargetProvider provider = provider(new TrackingOutputStream());
        CompletionException configurationFailure = assertThrows(CompletionException.class, () ->
            provider.open(request(target(Map.of("endpoint", "stdout", "root", "/tmp")))).toCompletableFuture().join());
        assertTrue(configurationFailure.getCause().getMessage().contains("no filesystem options"));

        ObjectWriteSession session = provider.open(request(target(Map.of("endpoint", "stdout"))))
            .toCompletableFuture().join();
        CompletionException abortFailure = assertThrows(CompletionException.class, () ->
            session.abort(new IllegalStateException("failed")).toCompletableFuture().join());
        assertTrue(abortFailure.getCause().getMessage().contains("cannot be aborted"));
    }

    @Test
    void writesRemainingBytesWithoutMutatingHeapOrReadOnlyBuffers() {
        TrackingOutputStream output = new TrackingOutputStream();
        ObjectWriteSession session = provider(output).open(request(target(Map.of("endpoint", "stdout"))))
            .toCompletableFuture().join();
        ByteBuffer heap = ByteBuffer.wrap("skip-keep".getBytes(StandardCharsets.UTF_8));
        heap.position("skip-".length());
        ByteBuffer readOnly = ByteBuffer.wrap("-read-only".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();

        session.write(heap).toCompletableFuture().join();
        session.write(readOnly).toCompletableFuture().join();

        assertEquals("keep-read-only", output.toString(StandardCharsets.UTF_8));
        assertEquals("skip-".length(), heap.position());
        assertEquals(0, readOnly.position());
    }

    private StdioObjectTargetProvider provider(TrackingOutputStream output) {
        return new StdioObjectTargetProvider(new StandardStreams(new ByteArrayInputStream(new byte[0]), output));
    }

    private PipelineObjectPublishConfig target(Map<String, Object> location) {
        return new PipelineObjectPublishConfig(
            "stdout-output",
            "object",
            "stdio",
            location,
            PipelineObjectNamingConfig.defaults(),
            PipelineObjectPublishPayloadConfig.defaults(),
            PipelineObjectPublishGroupingConfig.defaults());
    }

    private ObjectWriteOpenRequest request(PipelineObjectPublishConfig target) {
        return new ObjectWriteOpenRequest(
            target.name(), target, "result.json", "application/json", Map.of("test", "true"), "idempotency");
    }

    private static final class TrackingOutputStream extends ByteArrayOutputStream {
        private boolean flushed;
        private boolean closed;

        @Override
        public void flush() throws IOException {
            flushed = true;
            super.flush();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
