package org.pipelineframework.connector.objectingest;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.repository.PayloadReference;

/**
 * Filesystem object target provider for Object Publish.
 */
public class FilesystemObjectTargetProvider implements ObjectTargetProvider {
    private final Executor executor;

    public FilesystemObjectTargetProvider() {
        this(ForkJoinPool.commonPool());
    }

    FilesystemObjectTargetProvider(Executor executor) {
        this.executor = executor;
    }

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
        return CompletableFuture.supplyAsync(() -> openBlocking(request), executor);
    }

    private ObjectWriteSession openBlocking(ObjectWriteOpenRequest request) {
        try {
            Path root = root(request);
            Path finalPath = requireUnderRoot(root, root.resolve(request.objectKey()).normalize());
            Path parent = finalPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tempPath = Files.createTempFile(parent == null ? root : parent, ".tpf-publish-", ".tmp");
            OutputStream output = new BufferedOutputStream(Files.newOutputStream(tempPath));
            return new FilesystemWriteSession(request, root, finalPath, tempPath, output, executor);
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    private Path root(ObjectWriteOpenRequest request) {
        Object root = request.target().location().get("root");
        if (root == null || root.toString().isBlank()) {
            throw new IllegalArgumentException("filesystem publish target '" + request.targetName() + "' requires location.root");
        }
        return Path.of(root.toString()).toAbsolutePath().normalize();
    }

    private Path requireUnderRoot(Path root, Path path) {
        if (!path.startsWith(root)) {
            throw new SecurityException("Filesystem object publish path escapes configured root: " + path);
        }
        return path;
    }

    private static final class FilesystemWriteSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest request;
        private final Path root;
        private final Path finalPath;
        private final Path tempPath;
        private final OutputStream output;
        private final Executor executor;
        private boolean closed;

        private FilesystemWriteSession(
            ObjectWriteOpenRequest request,
            Path root,
            Path finalPath,
            Path tempPath,
            OutputStream output,
            Executor executor
        ) {
            this.request = request;
            this.root = root;
            this.finalPath = finalPath;
            this.tempPath = tempPath;
            this.output = output;
            this.executor = executor;
        }

        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            byte[] bytes = copy(chunk);
            return CompletableFuture.runAsync(() -> {
                try {
                    output.write(bytes);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    if (!closed) {
                        output.flush();
                        output.close();
                        closed = true;
                    }
                    Files.move(tempPath, finalPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
                    Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
                    metadata.putAll(closeRequest.metadata());
                    metadata.put("target", request.targetName());
                    PayloadReference reference = new PayloadReference(
                        "filesystem",
                        root.toString(),
                        request.objectKey(),
                        request.contentType(),
                        "raw",
                        closeRequest.checksum(),
                        closeRequest.bytes(),
                        null,
                        metadata);
                    return new ObjectWriteResult(reference, closeRequest.bytes(), closeRequest.checksum(), Instant.now());
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.runAsync(() -> {
                try {
                    if (!closed) {
                        output.close();
                        closed = true;
                    }
                    Files.deleteIfExists(tempPath);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }

        private static byte[] copy(ByteBuffer chunk) {
            if (chunk == null) {
                return new byte[0];
            }
            ByteBuffer duplicate = chunk.slice();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return bytes;
        }
    }
}
