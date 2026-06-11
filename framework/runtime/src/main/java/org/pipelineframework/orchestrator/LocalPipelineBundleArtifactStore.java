package org.pipelineframework.orchestrator;

import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.jar.JarFile;

/**
 * Local filesystem bundle artifact store for self-hosted and local/dev coordinators.
 */
public class LocalPipelineBundleArtifactStore implements PipelineBundleArtifactStore {

    private static final String SHA_256_PREFIX = "sha256:";

    private final Path root;
    private final PipelineBundleManifestLoader manifestLoader;

    public LocalPipelineBundleArtifactStore(Path root, PipelineBundleManifestLoader manifestLoader) {
        if (root == null) {
            throw new IllegalArgumentException("Bundle artifact store root is required");
        }
        this.root = root.toAbsolutePath().normalize();
        this.manifestLoader = manifestLoader == null ? new PipelineBundleManifestLoader() : manifestLoader;
    }

    @Override
    public PipelineBundleStoredArtifact store(Path sourcePath, PipelineBundleManifest manifest) {
        if (sourcePath == null || !Files.isRegularFile(sourcePath) || !Files.isReadable(sourcePath)) {
            throw new IllegalArgumentException("artifactPath must point to a readable bundle JAR");
        }
        if (manifest == null) {
            throw new IllegalArgumentException("Bundle manifest is required");
        }
        try {
            String checksum = checksum(sourcePath);
            long size = Files.size(sourcePath);
            Path target = artifactPath(checksum);
            Files.createDirectories(target.getParent());
            if (Files.exists(target)) {
                verifyStoredArtifact(target, size, checksum, manifest);
                return new PipelineBundleStoredArtifact(target.toString(), size, checksum);
            }
            Path temporary = Files.createTempFile(target.getParent(), "bundle-", ".tmp");
            try {
                Files.copy(sourcePath, temporary, StandardCopyOption.REPLACE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (FileAlreadyExistsException e) {
                    verifyStoredArtifact(target, size, checksum, manifest);
                    return new PipelineBundleStoredArtifact(target.toString(), size, checksum);
                } catch (AtomicMoveNotSupportedException e) {
                    try {
                        Files.move(temporary, target);
                    } catch (FileAlreadyExistsException alreadyStored) {
                        verifyStoredArtifact(target, size, checksum, manifest);
                        return new PipelineBundleStoredArtifact(target.toString(), size, checksum);
                    }
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
            verifyStoredArtifact(target, size, checksum, manifest);
            return new PipelineBundleStoredArtifact(target.toString(), size, checksum);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to store bundle artifact: " + e.getMessage(), e);
        }
    }

    @Override
    public void verify(PipelineBundleRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Bundle record is required");
        }
        Path artifact = Path.of(record.artifactPath());
        try {
            verifyStoredArtifact(
                artifact,
                record.artifactSizeBytes(),
                record.artifactChecksum(),
                record.manifest());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to verify stored bundle artifact: " + e.getMessage(), e);
        }
    }

    Path root() {
        return root;
    }

    private Path artifactPath(String checksum) {
        return root.resolve("artifacts").resolve(stripChecksumPrefix(checksum) + ".jar");
    }

    private void verifyStoredArtifact(
        Path artifact,
        long expectedSize,
        String expectedChecksum,
        PipelineBundleManifest expectedManifest) throws Exception {
        if (!Files.isRegularFile(artifact) || !Files.isReadable(artifact)) {
            throw new IllegalStateException("Stored bundle artifact is missing or unreadable");
        }
        if (expectedSize >= 0 && Files.size(artifact) != expectedSize) {
            throw new IllegalStateException("Stored bundle artifact size does not match registry metadata");
        }
        String actualChecksum = checksum(artifact);
        if (!actualChecksum.equals(expectedChecksum)) {
            throw new IllegalStateException("Stored bundle artifact checksum does not match registry metadata");
        }
        PipelineBundleManifest actualManifest = loadManifest(artifact);
        if (!actualManifest.pipelineId().equals(expectedManifest.pipelineId())
            || !actualManifest.bundleVersionId().equals(expectedManifest.bundleVersionId())
            || !actualManifest.bundleHash().equals(expectedManifest.bundleHash())) {
            throw new IllegalStateException("Stored bundle artifact manifest does not match registry metadata");
        }
    }

    private PipelineBundleManifest loadManifest(Path path) {
        try (JarFile jar = new JarFile(path.toFile())) {
            var entry = jar.getJarEntry(PipelineBundleManifest.RESOURCE_PATH);
            if (entry == null) {
                throw new IllegalStateException(
                    "Stored bundle artifact is missing " + PipelineBundleManifest.RESOURCE_PATH);
            }
            try (var stream = jar.getInputStream(entry)) {
                return manifestLoader.load(stream);
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read stored bundle manifest: " + e.getMessage(), e);
        }
    }

    private static String checksum(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                digest.update(buffer, 0, read);
            }
        }
        return SHA_256_PREFIX + HexFormat.of().formatHex(digest.digest());
    }

    private static String stripChecksumPrefix(String checksum) {
        if (checksum == null || checksum.isBlank()) {
            throw new IllegalArgumentException("Artifact checksum is required");
        }
        return checksum.startsWith(SHA_256_PREFIX)
            ? checksum.substring(SHA_256_PREFIX.length())
            : checksum;
    }
}
