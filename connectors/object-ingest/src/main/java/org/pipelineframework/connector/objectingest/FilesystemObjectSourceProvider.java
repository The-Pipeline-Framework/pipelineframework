package org.pipelineframework.connector.objectingest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectingest.ObjectSourceProvider;
import org.pipelineframework.repository.PayloadReference;

/**
 * Filesystem object source provider for local ingest and deterministic tests.
 */
public class FilesystemObjectSourceProvider implements ObjectSourceProvider {

    @Override
    public String providerName() {
        return "filesystem";
    }

    @Override
    public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
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
                .limit(Math.max(1, limit))
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
            if (maxBytes > 0 && Files.size(path) > maxBytes) {
                throw new IllegalStateException("Object exceeds configured maxBytes: " + item.key());
            }
            byte[] bytes = Files.readAllBytes(path);
            return Optional.of(new String(bytes, source.payload().charset()));
        } catch (IOException e) {
            throw new IllegalStateException("Failed reading filesystem object: " + item.key(), e);
        }
    }

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
                Map.of("source", source.name()));
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
}
