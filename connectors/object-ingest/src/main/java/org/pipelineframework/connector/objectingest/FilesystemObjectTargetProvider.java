package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.repository.PayloadReference;

/**
 * Filesystem object target provider for Object Publish.
 */
public class FilesystemObjectTargetProvider implements ObjectTargetProvider {

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public Uni<ObjectWriteResult> write(ObjectWriteRequest request) {
        return Uni.createFrom().item(() -> writeBlocking(request))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private ObjectWriteResult writeBlocking(ObjectWriteRequest request) {
        Path root = root(request);
        Path path = requireUnderRoot(root, root.resolve(request.objectKey()).normalize());
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.write(
                path,
                request.bytes(),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
            Map<String, String> metadata = new LinkedHashMap<>(request.metadata());
            metadata.put("target", request.targetName());
            PayloadReference reference = new PayloadReference(
                providerName(),
                root.toString(),
                request.objectKey(),
                request.contentType(),
                "raw",
                request.checksum(),
                request.bytes().length,
                null,
                metadata);
            return new ObjectWriteResult(reference, request.bytes().length, request.checksum(), Instant.now());
        } catch (IOException e) {
            throw new IllegalStateException("Failed writing filesystem object: " + request.objectKey(), e);
        }
    }

    private Path root(ObjectWriteRequest request) {
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
}
