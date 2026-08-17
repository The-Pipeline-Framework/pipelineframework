package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.repository.PayloadReference;

/**
 * Filesystem object source provider for local ingest and deterministic tests.
 */
public class FilesystemObjectSourceProvider implements ObjectSourceProvider {
    private final Executor executor;

    /**
     * Creates a filesystem object source provider using the common pool executor.
     */
    public FilesystemObjectSourceProvider() {
        this(ForkJoinPool.commonPool());
    }

    /**
     * Creates a filesystem object source provider using the specified executor.
     *
     * @param executor executor used for asynchronous filesystem operations
     * @throws NullPointerException if {@code executor} is {@code null}
     */
    FilesystemObjectSourceProvider(Executor executor) {
        this.executor = Objects.requireNonNull(executor, "filesystem source executor must not be null");
    }

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        Path root = root(source);
        Path prefix = prefix(source);
        Path listRoot = requireUnderRoot(root, root.resolve(prefix).normalize());
        if (!Files.isDirectory(listRoot)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.walk(listRoot)) {
            return paths
                .filter(Files::isRegularFile)
                .map(root::relativize)
                .map(Path::normalize)
                .map(path -> path.toString().replace('\\', '/'))
                .filter(key -> matches(source, key))
                .sorted()
                .limit(limit)
                .map(key -> item(source, root, key))
                .toList();
        } catch (IOException e) {
            throw new IllegalStateException("Failed listing filesystem object source: " + source.name(), e);
        }
    }

    /**
     * Reads a filesystem object as text using the configured payload charset.
     *
     * @param source  the source configuration
     * @param item    the object to read
     * @param maxBytes the maximum number of bytes to read
     * @return the object's decoded text
     */
    @Override
    public Optional<String> readText(PipelineObjectSourceConfig source, ObjectSourceItem item, long maxBytes) {
        Path root = root(source);
        Path path = requireUnderRoot(root, root.resolve(item.key()).normalize());
        try {
            byte[] bytes = readBounded(path, maxBytes, item.key());
            return Optional.of(new String(bytes, source.payload().charset()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading filesystem object: " + item.key(), e);
        }
    }

    /**
     * Materializes a referenced payload subject to the specified size limit.
     *
     * @param reference the payload reference to materialize
     * @param maxBytes the maximum number of bytes to read
     * @return the materialized payload
     */
    @Override
    public CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
        return CompletableFuture.supplyAsync(() -> materializeBlocking(reference, maxBytes), executor);
    }

    /**
     * Materializes a filesystem payload and verifies its content checksum when provided.
     *
     * @param reference the payload reference identifying the filesystem object
     * @param maxBytes the maximum number of bytes to read
     * @return the materialized payload with its computed checksum
     * @throws IllegalArgumentException if the reference, size limit, provider, or container is invalid
     * @throws IllegalStateException if the object exceeds the size limit or its checksum does not match
     * @throws CompletionException if reading the object fails
     */
    private MaterializedPayload materializeBlocking(PayloadReference reference, long maxBytes) {
        try {
            if (reference == null) {
                throw new IllegalArgumentException("payload reference must not be null");
            }
            if (maxBytes < 1) {
                throw new IllegalArgumentException("maxBytes must be positive");
            }
            if (!providerName().equalsIgnoreCase(reference.provider())) {
                throw new IllegalArgumentException("filesystem operation cannot materialize provider=" + reference.provider());
            }
            if (reference.container() == null || reference.container().isBlank()) {
                throw new IllegalArgumentException("filesystem payload reference container must not be blank");
            }
            if (reference.sizeBytes() > maxBytes) {
                throw new IllegalStateException("Object exceeds configured maxBytes: " + reference.key());
            }
            Path root = Path.of(reference.container()).toAbsolutePath().normalize();
            Path path = requireUnderRoot(root, root.resolve(reference.key()).normalize());
            byte[] bytes = readBounded(path, maxBytes, reference.key());
            String checksum = sha256(bytes);
            if (reference.checksum() != null && !reference.checksum().equalsIgnoreCase(checksum)) {
                throw new IllegalStateException("Filesystem payload checksum mismatch: " + reference.key());
            }
            return new MaterializedPayload(reference, bytes, reference.contentType(), reference.codec(), checksum);
        } catch (IOException failure) {
            throw new CompletionException(failure);
        }
    }

    /**
     * Reads a file into memory while enforcing an optional maximum byte limit.
     *
     * @param path     the file to read
     * @param maxBytes the maximum allowed content size, when positive
     * @param key      the object key used in the size-limit error message
     * @return the file contents
     * @throws IOException if the file cannot be read
     * @throws IllegalStateException if the content exceeds {@code maxBytes}
     */
    private byte[] readBounded(Path path, long maxBytes, String key) throws IOException {
        try (InputStream input = Files.newInputStream(path); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (maxBytes > 0 && (long) output.size() + read > maxBytes) {
                    throw new IllegalStateException("Object exceeds configured maxBytes: " + key);
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    /**
     * Builds an object source item containing filesystem metadata and a payload reference for the specified key.
     *
     * @param source the configured object source
     * @param root   the filesystem root containing the object
     * @param key    the object's path key relative to the root
     * @return the object source item for the specified key
     */
    private ObjectSourceItem item(PipelineObjectSourceConfig source, Path root, String key) {
        Path path = requireUnderRoot(root, root.resolve(key).normalize());
        try {
            long size = Files.size(path);
            long lastModified = Files.getLastModifiedTime(path).toMillis();
            String etag = sha256(path);
            String contentType = Files.probeContentType(path);
            PayloadReference reference = new PayloadReference(
                providerName(),
                root.toString(),
                key,
                contentType,
                "raw",
                etag,
                size,
                null,
                Map.of("source", source.name()),
                Optional.empty());
            return new ObjectSourceItem(
                providerName(),
                root.toString(),
                key,
                null,
                etag,
                size,
                lastModified,
                contentType,
                Map.of(),
                reference,
                localPath(source, root, key));
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading filesystem object metadata: " + path, e);
        }
    }

    private boolean matches(PipelineObjectSourceConfig source, String key) {
        boolean included = source.filter().include().isEmpty()
            || source.filter().include().stream().anyMatch(pattern -> glob(pattern).matches(Path.of(key)));
        boolean excluded = source.filter().exclude().stream().anyMatch(pattern -> glob(pattern).matches(Path.of(key)));
        return included && !excluded;
    }

    private PathMatcher glob(String pattern) {
        return FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }

    private Path root(PipelineObjectSourceConfig source) {
        Object root = source.location().get("root");
        if (root == null || root.toString().isBlank()) {
            throw new IllegalArgumentException("filesystem source '" + source.name() + "' requires location.root");
        }
        return Path.of(root.toString()).toAbsolutePath().normalize();
    }

    private Path prefix(PipelineObjectSourceConfig source) {
        Object prefix = source.location().get("prefix");
        return prefix == null || prefix.toString().isBlank()
            ? Path.of("")
            : Path.of(prefix.toString()).normalize();
    }

    /**
     * Resolves the path presented to downstream object-ingest mappers for a discovered object.
     *
     * <p>By default the presented path is resolved under the actual filesystem source {@code root}.
     * When {@code location.localPathRoot} is configured, the same object key is resolved under that
     * presentation root instead. This is useful when the source is discovered on the host but read
     * inside a mounted container path. In both cases the resolved path must remain under the selected
     * root, preventing path traversal through object keys.</p>
     *
     * @param source source configuration containing optional {@code location.localPathRoot}
     * @param root actual filesystem root used to discover source objects
     * @param key object key relative to the source root
     * @return path string presented in the object snapshot
     */
    private String localPath(PipelineObjectSourceConfig source, Path root, String key) {
        Object localPathRoot = source.location().get("localPathRoot");
        if (localPathRoot == null || localPathRoot.toString().isBlank()) {
            return requireUnderRoot(root, root.resolve(key).normalize()).toString();
        }
        Path localRoot = Path.of(localPathRoot.toString()).toAbsolutePath().normalize();
        return requireUnderRoot(localRoot, localRoot.resolve(key).normalize()).toString();
    }

    private Path requireUnderRoot(Path root, Path path) {
        if (!path.startsWith(root)) {
            throw new SecurityException("Filesystem object path escapes configured root: " + path);
        }
        return path;
    }

    /**
     * Computes the SHA-256 checksum of a file.
     *
     * @param path the file whose checksum is computed
     * @return the checksum as a lowercase hexadecimal string
     * @throws IOException if the file cannot be read
     */
    private String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream inputStream = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = inputStream.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }

    /**
     * Computes the SHA-256 digest of the supplied bytes as a hexadecimal string.
     *
     * @param bytes the bytes to digest
     * @return the hexadecimal SHA-256 digest
     */
    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }
}
