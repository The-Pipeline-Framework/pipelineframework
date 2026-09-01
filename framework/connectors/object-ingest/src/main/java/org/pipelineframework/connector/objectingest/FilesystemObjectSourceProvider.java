package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
import org.pipelineframework.step.NonRetryableException;

/**
 * Filesystem object source provider for local ingest and deterministic tests.
 */
public class FilesystemObjectSourceProvider implements ObjectSourceProvider {
    private static final String LOCATOR_DIGEST_METADATA = "tpf.filesystem.locator.sha256";
    private final Executor executor;

    public FilesystemObjectSourceProvider() {
        this(ForkJoinPool.commonPool());
    }

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

    @Override
    public CompletionStage<MaterializedPayload> materialize(PayloadReference reference, long maxBytes) {
        return CompletableFuture.supplyAsync(() -> materializeBlocking(reference, maxBytes), executor);
    }

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
                throw inputTooLarge(reference.key());
            }
            Path root = canonicalReferenceRoot(reference);
            String key = canonicalReferenceKey(reference.key());
            Path path = requireCanonicalUnderRoot(root, root.resolve(key), reference.key());
            verifyLocatorProvenance(reference, root, key, path);
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

    private byte[] readBounded(Path path, long maxBytes, String key) throws IOException {
        try (InputStream input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS);
                ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                if (maxBytes > 0 && (long) output.size() + read > maxBytes) {
                    throw inputTooLarge(key);
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static NonRetryableException inputTooLarge(String key) {
        return new NonRetryableException("Object exceeds configured maxBytes: " + key);
    }

    private ObjectSourceItem item(PipelineObjectSourceConfig source, Path root, String key) {
        try {
            Path path = requireCanonicalUnderRoot(root, root.resolve(key), key);
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
                Map.of(
                    "source", source.name(),
                    LOCATOR_DIGEST_METADATA, locatorDigest(root, key, path)),
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
            throw new IllegalStateException("Failed reading filesystem object metadata: " + root.resolve(key), e);
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
        Path configured = Path.of(root.toString()).toAbsolutePath().normalize();
        try {
            return configured.toRealPath();
        } catch (IOException failure) {
            throw new IllegalStateException(
                "Failed resolving canonical filesystem source root for '" + source.name() + "'", failure);
        }
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

    private Path canonicalReferenceRoot(PayloadReference reference) throws IOException {
        Path declared = Path.of(reference.container()).toAbsolutePath().normalize();
        Path canonical = declared.toRealPath();
        if (!declared.equals(canonical)) {
            throw new IllegalStateException("Filesystem payload root identity mismatch: " + reference.container());
        }
        return canonical;
    }

    private String canonicalReferenceKey(String key) {
        Path declared = Path.of(key);
        if (declared.isAbsolute()) {
            throw new IllegalStateException("Filesystem payload key must be relative: " + key);
        }
        String canonical = declared.normalize().toString().replace('\\', '/');
        if (!canonical.equals(key)) {
            throw new IllegalStateException("Filesystem payload key identity mismatch: " + key);
        }
        return canonical;
    }

    private Path requireCanonicalUnderRoot(Path root, Path path, String key) throws IOException {
        Path lexical = requireUnderRoot(root, path.toAbsolutePath().normalize());
        Path canonical = lexical.toRealPath();
        if (!canonical.startsWith(root)) {
            throw new SecurityException("Filesystem object path escapes canonical root: " + key);
        }
        return canonical;
    }

    private void verifyLocatorProvenance(PayloadReference reference, Path root, String key, Path path) {
        String expected = reference.metadata().get(LOCATOR_DIGEST_METADATA);
        String actual = locatorDigest(root, key, path);
        if (expected == null || !expected.equals(actual)) {
            throw new IllegalStateException("Filesystem payload locator provenance mismatch: " + reference.key());
        }
    }

    private String locatorDigest(Path root, String key, Path path) {
        return sha256((root + "\n" + key + "\n" + path).getBytes(StandardCharsets.UTF_8));
    }

    private String sha256(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream inputStream = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
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

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is not available", e);
        }
    }
}
