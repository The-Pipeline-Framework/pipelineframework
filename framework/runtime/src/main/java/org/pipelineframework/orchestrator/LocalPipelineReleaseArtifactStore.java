package org.pipelineframework.orchestrator;

import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.util.HexFormat;

import org.pipelineframework.orchestrator.release.PipelineReleaseRecord;

/**
 * Local filesystem release artifact store for self-hosted and local/dev coordinators.
 */
public class LocalPipelineReleaseArtifactStore implements PipelineReleaseArtifactStore {

    private static final String SHA_256_PREFIX = "sha256:";

    private final Path root;

    public LocalPipelineReleaseArtifactStore(Path root) {
        if (root == null) {
            throw new IllegalArgumentException("Release artifact store root is required");
        }
        this.root = root.toAbsolutePath().normalize();
    }

    @Override
    public PipelineReleaseStoredArtifact store(Path sourcePath) {
        if (sourcePath == null || !Files.isRegularFile(sourcePath) || !Files.isReadable(sourcePath)) {
            throw new IllegalArgumentException("artifactPath must point to a readable release artifact");
        }
        try {
            String checksum = checksum(sourcePath);
            long size = Files.size(sourcePath);
            Path target = artifactPath(checksum, sourcePath);
            Files.createDirectories(target.getParent());
            if (Files.exists(target)) {
                verifyStoredArtifact(target, size, checksum);
                return new PipelineReleaseStoredArtifact(target.toString(), size, checksum);
            }
            Path temporary = Files.createTempFile(target.getParent(), "release-artifact-", ".tmp");
            try {
                Files.copy(sourcePath, temporary, StandardCopyOption.REPLACE_EXISTING);
                try {
                    Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (FileAlreadyExistsException e) {
                    verifyStoredArtifact(target, size, checksum);
                    return new PipelineReleaseStoredArtifact(target.toString(), size, checksum);
                } catch (AtomicMoveNotSupportedException e) {
                    try {
                        Files.move(temporary, target);
                    } catch (FileAlreadyExistsException alreadyStored) {
                        verifyStoredArtifact(target, size, checksum);
                        return new PipelineReleaseStoredArtifact(target.toString(), size, checksum);
                    }
                }
            } finally {
                Files.deleteIfExists(temporary);
            }
            verifyStoredArtifact(target, size, checksum);
            return new PipelineReleaseStoredArtifact(target.toString(), size, checksum);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to store release artifact: " + e.getMessage(), e);
        }
    }

    @Override
    public void verify(PipelineReleaseRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Release record is required");
        }
        try {
            verifyStoredArtifact(
                Path.of(record.primaryArtifactPath()),
                record.primaryArtifactSizeBytes(),
                record.primaryArtifactChecksum());
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to verify stored release artifact: " + e.getMessage(), e);
        }
    }

    Path root() {
        return root;
    }

    private Path artifactPath(String checksum, Path sourcePath) {
        String filename = stripChecksumPrefix(checksum);
        String sourceName = sourcePath.getFileName() == null ? "" : sourcePath.getFileName().toString();
        int dot = sourceName.lastIndexOf('.');
        String extension = dot >= 0 ? sourceName.substring(dot) : ".artifact";
        return root.resolve("artifacts").resolve(filename + extension);
    }

    private void verifyStoredArtifact(Path artifact, long expectedSize, String expectedChecksum) throws Exception {
        if (!Files.isRegularFile(artifact) || !Files.isReadable(artifact)) {
            throw new IllegalStateException("Stored release artifact is missing or unreadable");
        }
        if (expectedSize >= 0 && Files.size(artifact) != expectedSize) {
            throw new IllegalStateException("Stored release artifact size does not match registry metadata");
        }
        if (!checksum(artifact).equals(expectedChecksum)) {
            throw new IllegalStateException("Stored release artifact checksum does not match registry metadata");
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
