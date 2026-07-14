package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.repository.PayloadReference;

/**
 * Non-closing object publish target backed by standard output.
 */
public final class StdioObjectTargetProvider implements ObjectTargetProvider {
    private static final String ENDPOINT = "stdout";
    private final StandardStreams streams;

    public StdioObjectTargetProvider() {
        this(StandardStreams.jvm());
    }

    StdioObjectTargetProvider(StandardStreams streams) {
        this.streams = Objects.requireNonNull(streams, "streams");
    }

    @Override
    public String providerName() {
        return "stdio";
    }

    @Override
    public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
        try {
            validate(request.target());
            return CompletableFuture.completedFuture(new StdioWriteSession(request, streams));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void validate(PipelineObjectPublishConfig target) {
        if (!Map.of("endpoint", ENDPOINT).equals(target.location())) {
            throw new IllegalArgumentException("stdio stdout target requires location.endpoint=stdout and no filesystem options");
        }
    }

    private static final class StdioWriteSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest request;
        private final StandardStreams streams;
        private boolean closed;

        private StdioWriteSession(ObjectWriteOpenRequest request, StandardStreams streams) {
            this.request = request;
            this.streams = streams;
        }

        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            synchronized (streams) {
                if (closed) {
                    return CompletableFuture.failedFuture(new IllegalStateException("stdout write session is closed"));
                }
                byte[] bytes = new byte[chunk.remaining()];
                chunk.get(bytes);
                try {
                    streams.stdout().write(bytes);
                    return CompletableFuture.completedFuture(null);
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            synchronized (streams) {
                try {
                    if (!closed) {
                        streams.stdout().flush();
                        closed = true;
                    }
                    Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
                    metadata.putAll(closeRequest.metadata());
                    metadata.put("endpoint", ENDPOINT);
                    PayloadReference reference = new PayloadReference(
                        "stdio", ENDPOINT, request.objectKey(), request.contentType(), "raw", closeRequest.checksum(), closeRequest.bytes(), null, metadata);
                    return CompletableFuture.completedFuture(new ObjectWriteResult(reference, closeRequest.bytes(), closeRequest.checksum(), Instant.now()));
                } catch (IOException | RuntimeException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException(
                "stdout is non-atomic; emitted bytes cannot be aborted or retracted"));
        }
    }
}
