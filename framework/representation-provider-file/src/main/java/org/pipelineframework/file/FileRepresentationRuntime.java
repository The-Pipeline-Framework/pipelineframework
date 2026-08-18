package org.pipelineframework.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.connector.ConnectorBindingName;
import org.pipelineframework.connector.ConnectorBindingRegistry;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.PayloadMaterializer;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectTargetRegistry;
import org.pipelineframework.objectpublish.ObjectWriteRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.repository.PayloadReference;

/** Runtime support for provider-generated {@link Path} facades. */
@ApplicationScoped
public final class FileRepresentationRuntime {
    private final Supplier<PayloadMaterializer> materializer;
    private final ConnectorBindingRegistry connectorBindings;
    private volatile PipelineYamlConfig config;
    private volatile ObjectTargetRegistry targets;

    @Inject
    public FileRepresentationRuntime(Instance<PayloadMaterializer> materializers,
                                     ConnectorBindingRegistry connectorBindings) {
        Objects.requireNonNull(materializers, "materializers");
        this.materializer = () -> {
            if (!materializers.isResolvable()) {
                throw new IllegalStateException("exactly one PayloadMaterializer is required for file representations");
            }
            return materializers.get();
        };
        this.connectorBindings = Objects.requireNonNull(connectorBindings, "connectorBindings");
    }

    FileRepresentationRuntime(PayloadMaterializer materializer, ConnectorBindingRegistry connectorBindings,
                              PipelineYamlConfig config, ObjectTargetRegistry targets) {
        this.materializer = () -> Objects.requireNonNull(materializer, "materializer");
        this.connectorBindings = Objects.requireNonNull(connectorBindings, "connectorBindings");
        this.config = Objects.requireNonNull(config, "config");
        this.targets = Objects.requireNonNull(targets, "targets");
    }

    public Uni<PayloadReference> oneToOne(PayloadReference input, long inputMaxBytes,
                                          String targetName, long outputMaxBytes, Optional<String> objectKey,
                                          Function<Path, Uni<Path>> delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return prepare(input, inputMaxBytes).chain(workspace -> {
            Uni<Path> result = Objects.requireNonNull(delegate.apply(workspace.input()),
                "file service returned a null Uni");
            return result.chain(path -> publish(workspace, path, targetName, outputMaxBytes, objectKey))
                .eventually(() -> cleanup(workspace));
        });
    }

    public Multi<PayloadReference> oneToMany(PayloadReference input, long inputMaxBytes,
                                             String targetName, long outputMaxBytes, Optional<String> objectKey,
                                             Function<Path, Multi<Path>> delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return prepare(input, inputMaxBytes).onItem().transformToMulti(workspace -> {
            Multi<Path> results = Objects.requireNonNull(delegate.apply(workspace.input()),
                "file service returned a null Multi");
            return results.onItem()
                .transformToUniAndConcatenate(path -> publish(workspace, path, targetName, outputMaxBytes, objectKey))
                .onTermination().call(() -> cleanup(workspace));
        });
    }

    private Uni<Workspace> prepare(PayloadReference reference, long maxBytes) {
        requirePositive(maxBytes, "input maxBytes");
        Objects.requireNonNull(reference, "input reference");
        return Uni.createFrom().completionStage(() -> materializer.get().materialize(reference, maxBytes))
            .chain(payload -> blocking(() -> stage(reference, payload, maxBytes)));
    }

    private Workspace stage(PayloadReference requested, MaterializedPayload payload, long maxBytes) {
        if (!requested.equals(payload.reference())) {
            throw new IllegalStateException("materializer returned a different payload reference");
        }
        byte[] bytes = payload.bytes();
        if (bytes.length > maxBytes) {
            throw new IllegalStateException("materialized payload exceeds maxBytes: " + bytes.length + " > " + maxBytes);
        }
        Path root = null;
        try {
            root = Files.createTempDirectory("tpf-file-").toRealPath();
            Path inputDirectory = Files.createDirectory(root.resolve("input"));
            Files.createDirectory(root.resolve("output"));
            Path input = inputDirectory.resolve(safeFilename(requested.key())).normalize();
            Files.write(input, bytes);
            return new Workspace(root, input);
        } catch (IOException e) {
            if (root != null) {
                try {
                    deleteRecursively(root);
                } catch (IOException cleanupFailure) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw new IllegalStateException("failed to stage materialized payload", e);
        }
    }

    private Uni<PayloadReference> publish(Workspace workspace, Path output, String targetName, long maxBytes,
                                          Optional<String> configuredKey) {
        requirePositive(maxBytes, "output maxBytes");
        Objects.requireNonNull(output, "file service output");
        Objects.requireNonNull(configuredKey, "configuredKey");
        return blocking(() -> readOutput(workspace, output, maxBytes, configuredKey))
            .chain(staged -> {
                PipelineObjectPublishConfig target = requireTarget(targetName);
                ObjectTargetProvider provider = targetRegistry().require(target.provider());
                String checksum = sha256(staged.bytes());
                ObjectWriteRequest request = new ObjectWriteRequest(
                    target.name(), target, staged.objectKey(), staged.bytes(), target.payload().contentType(),
                    Map.of("representation", "file"), checksum,
                    "file-representation:" + target.name() + ":" + staged.objectKey() + ":" + checksum);
                return Uni.createFrom().completionStage(() -> provider.write(request))
                    .map(result -> ownedReference(target, provider, result));
            });
    }

    private PayloadReference ownedReference(PipelineObjectPublishConfig target, ObjectTargetProvider provider,
                                            ObjectWriteResult result) {
        PayloadReference reference = Objects.requireNonNull(result.reference(),
            "object target returned no payload reference");
        ConnectorBindingName binding = ConnectorBindingName.of(target.binding().orElseThrow(() ->
            new IllegalStateException("file representation target '" + target.name()
                + "' must declare its connector binding")));
        return connectorBindings.ownPayloadReference(binding, provider.id(), provider.majorVersion(), reference);
    }

    private StagedOutput readOutput(Workspace workspace, Path output, long maxBytes, Optional<String> configuredKey) {
        try {
            Path real = output.toRealPath();
            if (!real.startsWith(workspace.root()) || !Files.isRegularFile(real, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalStateException("file service output must be a regular file inside its workspace");
            }
            long size = Files.size(real);
            if (size > maxBytes) {
                throw new IllegalStateException("file service output exceeds maxBytes: " + size + " > " + maxBytes);
            }
            String key = configuredKey.filter(value -> !value.isBlank()).orElseGet(() -> real.getFileName().toString());
            byte[] bytes = Files.readAllBytes(real);
            if (bytes.length > maxBytes) {
                throw new IllegalStateException("file service output exceeds maxBytes: " + bytes.length + " > " + maxBytes);
            }
            return new StagedOutput(key, bytes);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read file service output", e);
        }
    }

    private PipelineObjectPublishConfig requireTarget(String targetName) {
        if (targetName == null || targetName.isBlank()) {
            throw new IllegalArgumentException("file representation output target must not be blank");
        }
        PipelineObjectPublishConfig target = configuration().publish().get(targetName.trim());
        if (target == null) {
            throw new IllegalStateException("file representation publish target is not configured: " + targetName);
        }
        return target;
    }

    private PipelineYamlConfig configuration() {
        PipelineYamlConfig resolved = config;
        if (resolved == null) {
            synchronized (this) {
                resolved = config;
                if (resolved == null) {
                    Path path = new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath())
                        .orElseThrow(() -> new IllegalStateException("pipeline configuration is required for file publication"));
                    resolved = new PipelineYamlConfigLoader().load(path);
                    config = resolved;
                }
            }
        }
        return resolved;
    }

    private ObjectTargetRegistry targetRegistry() {
        ObjectTargetRegistry resolved = targets;
        if (resolved == null) {
            synchronized (this) {
                resolved = targets;
                if (resolved == null) {
                    resolved = ObjectTargetRegistry.load();
                    targets = resolved;
                }
            }
        }
        return resolved;
    }

    private static <T> Uni<T> blocking(java.util.concurrent.Callable<T> action) {
        return Uni.createFrom().item(() -> {
            try {
                return action.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static Uni<Void> cleanup(Workspace workspace) {
        return blocking(() -> {
            deleteRecursively(workspace.root());
            return Boolean.TRUE;
        }).replaceWithVoid();
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static String safeFilename(String key) {
        if (key == null || key.isBlank()) {
            return "payload.bin";
        }
        Path filename = Path.of(key.replace('\\', '/')).getFileName();
        String value = filename == null ? "payload.bin" : filename.toString();
        return value.isBlank() || ".".equals(value) || "..".equals(value) ? "payload.bin" : value;
    }

    private static void requirePositive(long value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be > 0");
        }
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private record Workspace(Path root, Path input) {
    }

    private record StagedOutput(String objectKey, byte[] bytes) {
        private StagedOutput {
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }
}
