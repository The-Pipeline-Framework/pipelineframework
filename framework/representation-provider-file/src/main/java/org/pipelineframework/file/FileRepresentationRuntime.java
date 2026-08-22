package org.pipelineframework.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import jakarta.enterprise.context.ApplicationScoped;
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
    public FileRepresentationRuntime(ConnectorBindingRegistry connectorBindings) {
        this.connectorBindings = Objects.requireNonNull(connectorBindings, "connectorBindings");
        this.materializer = () -> connectorBindings::materialize;
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
        return prepare(input, inputMaxBytes).chain(workspace ->
            Uni.createFrom().deferred(() -> {
                Uni<Path> result = Objects.requireNonNull(delegate.apply(workspace.input()),
                    "file service returned a null Uni");
                return result.chain(path -> publish(workspace, path, targetName, outputMaxBytes, objectKey));
            }).eventually(() -> cleanup(workspace)));
    }

    public Multi<PayloadReference> oneToMany(PayloadReference input, long inputMaxBytes,
                                             String targetName, long outputMaxBytes, Optional<String> objectKey,
                                             Function<Path, Multi<Path>> delegate) {
        Objects.requireNonNull(delegate, "delegate");
        return prepare(input, inputMaxBytes).onItem().transformToMulti(workspace ->
            Multi.createFrom().deferred(() -> {
                Multi<Path> results = Objects.requireNonNull(delegate.apply(workspace.input()),
                    "file service returned a null Multi");
                return results.onItem()
                    .transformToUniAndConcatenate(path -> publish(workspace, path, targetName, outputMaxBytes, objectKey))
                    .onTermination().call(() -> cleanup(workspace));
            }));
    }

    /**
     * Materializes a finite named set of canonical payload references for an ordinary typed service.
     * The byte limit applies to the set as a whole and all staged files share one invocation workspace.
     */
    public <T> Uni<T> withMaterialized(Map<String, PayloadReference> inputs, long maxBytes,
                                       Function<Map<String, Path>, Uni<T>> delegate) {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(delegate, "delegate");
        requirePositive(maxBytes, "input maxBytes");
        if (inputs.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("file inputs must not be empty"));
        }
        LinkedHashMap<String, PayloadReference> ordered = new LinkedHashMap<>();
        inputs.forEach((field, reference) -> {
            if (field == null || field.isBlank()) {
                throw new IllegalArgumentException("file input field names must not be blank");
            }
            ordered.put(field, Objects.requireNonNull(reference, "file input reference"));
        });
        return materialize(List.copyOf(ordered.entrySet()), 0, maxBytes, new LinkedHashMap<>())
            .chain(materialized -> blocking(() -> stage(materialized, maxBytes)))
            .chain(workspace -> Uni.createFrom().deferred(() ->
                Objects.requireNonNull(delegate.apply(workspace.inputs()),
                    "file service returned a null Uni"))
                .eventually(() -> cleanup(workspace.root())));
    }

    /** Materializes structured inputs, invokes an authored transform, and publishes its file fields. */
    public <R, O> Uni<O> transformStructured(
        Map<String, PayloadReference> inputs,
        long inputMaxBytes,
        String targetName,
        long outputMaxBytes,
        Function<Map<String, Path>, Uni<R>> delegate,
        Function<R, Map<String, Path>> outputs,
        BiFunction<R, Map<String, PayloadReference>, O> canonicalizer
    ) {
        Objects.requireNonNull(delegate, "delegate");
        Objects.requireNonNull(outputs, "outputs");
        Objects.requireNonNull(canonicalizer, "canonicalizer");
        requirePositive(outputMaxBytes, "output maxBytes");
        return materializeStructured(inputs, inputMaxBytes)
            .chain(workspace -> Uni.createFrom().deferred(() ->
                Objects.requireNonNull(delegate.apply(workspace.inputs()), "file service returned a null Uni")
                    .chain(result -> publishStructured(
                        workspace, result, outputs.apply(result), targetName, outputMaxBytes, canonicalizer)))
                .eventually(() -> cleanup(workspace.root())));
    }

    private Uni<StructuredWorkspace> materializeStructured(Map<String, PayloadReference> inputs, long maxBytes) {
        Objects.requireNonNull(inputs, "inputs");
        requirePositive(maxBytes, "input maxBytes");
        if (inputs.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("file inputs must not be empty"));
        }
        LinkedHashMap<String, PayloadReference> ordered = new LinkedHashMap<>();
        inputs.forEach((field, reference) -> {
            if (field == null || field.isBlank()) {
                throw new IllegalArgumentException("file input field names must not be blank");
            }
            ordered.put(field, Objects.requireNonNull(reference, "file input reference"));
        });
        return materialize(List.copyOf(ordered.entrySet()), 0, maxBytes, new LinkedHashMap<>())
            .chain(materialized -> blocking(() -> stage(materialized, maxBytes)));
    }

    private <R, O> Uni<O> publishStructured(
        StructuredWorkspace workspace,
        R result,
        Map<String, Path> outputs,
        String targetName,
        long outputMaxBytes,
        BiFunction<R, Map<String, PayloadReference>, O> canonicalizer
    ) {
        Objects.requireNonNull(outputs, "structured file service outputs");
        if (outputs.isEmpty()) {
            return Uni.createFrom().failure(new IllegalArgumentException("structured file outputs must not be empty"));
        }
        LinkedHashMap<String, Path> ordered = new LinkedHashMap<>();
        outputs.forEach((field, path) -> {
            if (field == null || field.isBlank()) {
                throw new IllegalArgumentException("file output field names must not be blank");
            }
            ordered.put(field, Objects.requireNonNull(path, "file output path"));
        });
        return publishStructuredField(workspace, List.copyOf(ordered.entrySet()), 0, targetName,
            outputMaxBytes, new LinkedHashMap<>())
            .map(references -> canonicalizer.apply(result, references));
    }

    private Uni<Map<String, PayloadReference>> publishStructuredField(
        StructuredWorkspace workspace,
        List<Map.Entry<String, Path>> outputs,
        int index,
        String targetName,
        long outputMaxBytes,
        LinkedHashMap<String, PayloadReference> references
    ) {
        if (index == outputs.size()) {
            return Uni.createFrom().item(Collections.unmodifiableMap(new LinkedHashMap<>(references)));
        }
        Map.Entry<String, Path> output = outputs.get(index);
        Workspace publicationWorkspace = new Workspace(workspace.root(), workspace.root());
        String objectKey = output.getKey() + "/" + output.getValue().getFileName();
        return publish(publicationWorkspace, output.getValue(), targetName, outputMaxBytes, Optional.of(objectKey))
            .chain(reference -> {
                references.put(output.getKey(), reference);
                return publishStructuredField(workspace, outputs, index + 1, targetName,
                    outputMaxBytes, references);
            });
    }

    @SafeVarargs
    public static Map<String, PayloadReference> orderedInputs(
        Map.Entry<String, PayloadReference>... inputs
    ) {
        LinkedHashMap<String, PayloadReference> ordered = new LinkedHashMap<>();
        for (Map.Entry<String, PayloadReference> input : inputs) {
            ordered.put(input.getKey(), input.getValue());
        }
        return Collections.unmodifiableMap(ordered);
    }

    @SafeVarargs
    public static Map<String, Path> orderedOutputs(Map.Entry<String, Path>... outputs) {
        LinkedHashMap<String, Path> ordered = new LinkedHashMap<>();
        for (Map.Entry<String, Path> output : outputs) {
            ordered.put(output.getKey(), output.getValue());
        }
        return Collections.unmodifiableMap(ordered);
    }

    private Uni<Map<String, MaterializedInput>> materialize(
        List<Map.Entry<String, PayloadReference>> inputs,
        int index,
        long remainingBytes,
        LinkedHashMap<String, MaterializedInput> materialized
    ) {
        if (index == inputs.size()) {
            return Uni.createFrom().item(Collections.unmodifiableMap(new LinkedHashMap<>(materialized)));
        }
        Map.Entry<String, PayloadReference> input = inputs.get(index);
        PayloadReference requested = input.getValue();
        long requestBudget = Math.max(remainingBytes, 1L);
        return Uni.createFrom().completionStage(() -> materializer.get().materialize(requested, requestBudget))
            .chain(payload -> {
                if (!requested.equals(payload.reference())) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "materializer returned a different payload reference for field '" + input.getKey() + "'"));
                }
                byte[] bytes = payload.bytes();
                if (bytes.length > remainingBytes) {
                    return Uni.createFrom().failure(new IllegalStateException(
                        "materialized file inputs exceed maxBytes: " + bytes.length + " > " + remainingBytes));
                }
                materialized.put(input.getKey(), new MaterializedInput(requested, bytes));
                return materialize(inputs, index + 1, remainingBytes - bytes.length, materialized);
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
        String safeName = safeFilename(requested.key());
        return withWorkspace("tpf-file-", "failed to stage materialized payload", workspace -> {
            Path inputDirectory = workspace.input();
            Path input = inputDirectory.resolve(safeName).normalize();
            Files.write(input, bytes);
            return new Workspace(workspace.root(), input);
        });
    }

    private StructuredWorkspace stage(Map<String, MaterializedInput> materialized, long maxBytes) {
        long actualBytes = materialized.values().stream().mapToLong(MaterializedInput::length).sum();
        if (actualBytes > maxBytes) {
            throw new IllegalStateException("materialized file inputs exceed maxBytes: "
                + actualBytes + " > " + maxBytes);
        }
        return withWorkspace("tpf-files-", "failed to stage materialized file inputs", workspace -> {
            LinkedHashMap<String, Path> inputs = new LinkedHashMap<>();
            int fieldIndex = 0;
            for (Map.Entry<String, MaterializedInput> entry : materialized.entrySet()) {
                String directoryName = fieldIndex++ + "-" + safeFieldName(entry.getKey());
                Path fieldDirectory = Files.createDirectory(workspace.input().resolve(directoryName));
                Path input = fieldDirectory.resolve(safeFilename(entry.getValue().reference().key())).normalize();
                byte[] bytes = entry.getValue().bytes();
                Files.write(input, bytes);
                inputs.put(entry.getKey(), input);
            }
            return new StructuredWorkspace(workspace.root(), inputs);
        });
    }

    private static <T> T withWorkspace(String prefix, String failureMessage, WorkspaceBody<T> body) {
        Path root = null;
        try {
            root = Files.createTempDirectory(prefix).toRealPath();
            Path input = Files.createDirectory(root.resolve("input"));
            Path output = Files.createDirectory(root.resolve("output"));
            return body.apply(new WorkspaceDirectories(root, input, output));
        } catch (IOException e) {
            cleanupAfterFailure(root, e);
            throw new IllegalStateException(failureMessage, e);
        } catch (RuntimeException e) {
            cleanupAfterFailure(root, e);
            throw e;
        }
    }

    private static void cleanupAfterFailure(Path root, Exception failure) {
        if (root == null) {
            return;
        }
        try {
            deleteRecursively(root);
        } catch (IOException cleanupFailure) {
            failure.addSuppressed(cleanupFailure);
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
            byte[] bytes = readOutputBytes(real, maxBytes);
            if (bytes.length > maxBytes) {
                throw new IllegalStateException("file service output exceeds maxBytes: " + bytes.length + " > " + maxBytes);
            }
            return new StagedOutput(key, bytes);
        } catch (IOException e) {
            throw new IllegalStateException("failed to read file service output", e);
        }
    }

    private static byte[] readOutputBytes(Path output, long maxBytes) throws IOException {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException("maxBytes must be > 0");
        }
        if (maxBytes >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("output maxBytes must be smaller than " + Integer.MAX_VALUE);
        }
        int maxRead = Math.toIntExact(maxBytes + 1L);
        try (var stream = Files.newInputStream(output)) {
            java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int remaining = maxRead;
            while (remaining > 0) {
                int read = stream.read(buffer, 0, Math.min(buffer.length, remaining));
                if (read < 0) {
                    break;
                }
                bytes.write(buffer, 0, read);
                remaining -= read;
            }
            if (remaining == 0 && stream.read() != -1) {
                throw new IllegalStateException("file service output exceeds maxBytes: " + (maxBytes + 1) + " > " + maxBytes);
            }
            return bytes.toByteArray();
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
        return cleanup(workspace.root());
    }

    private static Uni<Void> cleanup(Path root) {
        return blocking(() -> {
            deleteRecursively(root);
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

    private static String safeFieldName(String field) {
        String value = field.replaceAll("[^A-Za-z0-9._-]", "_");
        return value.isBlank() || ".".equals(value) || "..".equals(value) ? "payload" : value;
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

    private record WorkspaceDirectories(Path root, Path input, Path output) {
    }

    @FunctionalInterface
    private interface WorkspaceBody<T> {
        T apply(WorkspaceDirectories workspace) throws IOException;
    }

    private record StructuredWorkspace(Path root, Map<String, Path> inputs) {
        private StructuredWorkspace {
            inputs = Collections.unmodifiableMap(new LinkedHashMap<>(inputs));
        }
    }

    private record MaterializedInput(PayloadReference reference, byte[] bytes) {
        private MaterializedInput {
            Objects.requireNonNull(reference, "reference");
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }

        private int length() {
            return bytes.length;
        }
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
