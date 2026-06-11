package org.pipelineframework.orchestrator;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.jar.JarFile;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Validates build-produced release descriptors before local/dev registration.
 */
@ApplicationScoped
public class PipelineReleaseRegistrar {

    private static final Set<String> SUPPORTED_KINDS = Set.of(
        "local-file",
        "jar",
        "native-binary",
        "container-image",
        "lambda-zip",
        "lambda-image",
        "external-endpoint");

    @Inject
    PipelineReleaseDescriptorLoader descriptorLoader;

    @Inject
    PipelineBundleManifestLoader manifestLoader;

    @Inject
    PipelineBundleArtifactStore artifactStore;

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    public PipelineReleaseRecord validate(
        String tenantId,
        String pipelineId,
        String releaseDescriptorPath,
        long nowEpochMs) {
        validateRequired("tenantId", tenantId);
        validateRequired("pipelineId", pipelineId);
        validateRequired("releaseDescriptorPath", releaseDescriptorPath);
        Path descriptorPath = Path.of(releaseDescriptorPath);
        if (!descriptorPath.isAbsolute()) {
            throw new IllegalArgumentException("releaseDescriptorPath must be absolute");
        }
        PipelineReleaseDescriptor descriptor = loader().load(descriptorPath);
        validateDescriptorIdentity(pipelineId, descriptor);
        ValidatedArtifact primary = validateArtifacts(descriptor);
        return new PipelineReleaseRecord(
            tenantId.trim(),
            pipelineId.trim(),
            descriptor.contractVersion(),
            descriptor.releaseVersion(),
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            primary.bundleVersionId(),
            primary.bundleHash(),
            primary.artifactPath(),
            primary.artifactSizeBytes(),
            primary.artifactChecksum(),
            primary.manifest().orElse(null),
            nowEpochMs,
            nowEpochMs,
            0L);
    }

    public Uni<PipelineReleaseRecord> validateAsync(
        String tenantId,
        String pipelineId,
        String releaseDescriptorPath,
        long nowEpochMs) {
        return Uni.createFrom().item(() -> validate(tenantId, pipelineId, releaseDescriptorPath, nowEpochMs))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    public void verify(PipelineReleaseRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Release record is required");
        }
        if (record.primaryArtifactPath() != null && !record.primaryArtifactPath().isBlank()) {
            Path artifactPath = Path.of(record.primaryArtifactPath());
            if (!Files.isRegularFile(artifactPath) || !Files.isReadable(artifactPath)) {
                throw new IllegalStateException("Active release artifact is missing or unreadable");
            }
            String checksum = "sha256:" + sha256(artifactPath);
            if (!checksum.equals(record.primaryArtifactChecksum())) {
                throw new IllegalStateException("Active release artifact checksum does not match registered metadata");
            }
            if (record.manifest() != null) {
                artifactStore().verify(toBundleRecord(record));
            }
        }
    }

    private void validateDescriptorIdentity(String expectedPipelineId, PipelineReleaseDescriptor descriptor) {
        if (!expectedPipelineId.trim().equals(descriptor.pipelineId().trim())) {
            throw new IllegalArgumentException("Release descriptor pipelineId does not match request path");
        }
        validateRequired("contractVersion", descriptor.contractVersion());
        validateRequired("releaseVersion", descriptor.releaseVersion());
        if (descriptor.artifacts().isEmpty()) {
            throw new IllegalArgumentException("Release descriptor must contain at least one artifact");
        }
    }

    private ValidatedArtifact validateArtifacts(PipelineReleaseDescriptor descriptor) {
        ValidatedArtifact primary = null;
        for (PipelineReleaseArtifactDescriptor artifact : descriptor.artifacts()) {
            validateArtifactDescriptor(artifact);
            ValidatedArtifact validated = switch (artifact.kind().toLowerCase(Locale.ROOT)) {
                case "jar" -> validateJarArtifact(descriptor, artifact);
                case "local-file", "native-binary", "lambda-zip" -> validateLocalFileArtifact(artifact);
                case "container-image", "lambda-image", "external-endpoint" -> ValidatedArtifact.remote();
                default -> throw new IllegalArgumentException("Unsupported release artifact kind " + artifact.kind());
            };
            if (primary == null || validated.manifest().isPresent()) {
                primary = validated;
            }
        }
        return primary == null ? ValidatedArtifact.remote() : primary;
    }

    private void validateArtifactDescriptor(PipelineReleaseArtifactDescriptor artifact) {
        validateRequired("artifactId", artifact.artifactId());
        validateRequired("kind", artifact.kind());
        validateRequired("uri", artifact.uri());
        validateRequired("digest", artifact.digest());
        if (!SUPPORTED_KINDS.contains(artifact.kind().toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("Unsupported release artifact kind " + artifact.kind());
        }
        if (!artifact.digest().startsWith("sha256:")) {
            throw new IllegalArgumentException("Release artifact digest must use sha256:<hex>");
        }
    }

    private ValidatedArtifact validateJarArtifact(
        PipelineReleaseDescriptor descriptor,
        PipelineReleaseArtifactDescriptor artifact) {
        Path artifactPath = localPath(artifact.uri());
        if (!Files.isRegularFile(artifactPath) || !Files.isReadable(artifactPath)) {
            throw new IllegalArgumentException("Release JAR artifact must point to a readable local file");
        }
        String checksum = validateDigest(artifact, artifactPath);
        PipelineBundleManifest manifest = loadManifestIfPresent(artifactPath)
            .orElseThrow(() -> new IllegalArgumentException(
                "Release JAR artifact is missing " + PipelineBundleManifest.RESOURCE_PATH));
        validateManifestCompatibility(descriptor, artifact, manifest);
        PipelineBundleStoredArtifact stored = artifactStore().store(artifactPath, manifest);
        return new ValidatedArtifact(
            stored.artifactPath(),
            stored.artifactSizeBytes(),
            stored.artifactChecksum(),
            manifest.bundleVersionId(),
            manifest.bundleHash(),
            Optional.of(manifest));
    }

    private ValidatedArtifact validateLocalFileArtifact(PipelineReleaseArtifactDescriptor artifact) {
        Path artifactPath = localPath(artifact.uri());
        if (!Files.isRegularFile(artifactPath) || !Files.isReadable(artifactPath)) {
            throw new IllegalArgumentException("Release local artifact must point to a readable file");
        }
        String checksum = validateDigest(artifact, artifactPath);
        return new ValidatedArtifact(
            artifactPath.toAbsolutePath().normalize().toString(),
            size(artifactPath),
            "sha256:" + checksum,
            artifact.bundleVersionId(),
            artifact.bundleHash(),
            Optional.empty());
    }

    private Optional<PipelineBundleManifest> loadManifestIfPresent(Path path) {
        try (JarFile jar = new JarFile(path.toFile())) {
            var entry = jar.getJarEntry(PipelineBundleManifest.RESOURCE_PATH);
            if (entry == null) {
                return Optional.empty();
            }
            try (InputStream stream = jar.getInputStream(entry)) {
                return Optional.of(manifestLoader().load(stream));
            }
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to inspect release JAR artifact: " + e.getMessage(), e);
        }
    }

    private void validateManifestCompatibility(
        PipelineReleaseDescriptor descriptor,
        PipelineReleaseArtifactDescriptor artifact,
        PipelineBundleManifest manifest) {
        if (!descriptor.pipelineId().equals(manifest.pipelineId())) {
            throw new IllegalArgumentException("Release artifact manifest pipelineId does not match release descriptor");
        }
        if (!artifact.bundleVersionId().isBlank() && !artifact.bundleVersionId().equals(manifest.bundleVersionId())) {
            throw new IllegalArgumentException("Release artifact bundleVersionId does not match manifest");
        }
        if (!artifact.bundleHash().isBlank() && !artifact.bundleHash().equals(manifest.bundleHash())) {
            throw new IllegalArgumentException("Release artifact bundleHash does not match manifest");
        }
        if (!descriptor.contractVersion().equals("sha256:" + manifest.bundleHash())) {
            throw new IllegalArgumentException(
                "Release contractVersion is incompatible with the JAR manifest identity");
        }
    }

    private String validateDigest(PipelineReleaseArtifactDescriptor artifact, Path path) {
        String checksum = sha256(path);
        if (!artifact.digest().equals("sha256:" + checksum)) {
            throw new IllegalArgumentException("Release artifact digest does not match local file checksum");
        }
        return checksum;
    }

    private PipelineBundleRecord toBundleRecord(PipelineReleaseRecord record) {
        return new PipelineBundleRecord(
            record.tenantId(),
            record.pipelineId(),
            record.bundleVersionId(),
            record.bundleHash(),
            record.primaryArtifactPath(),
            record.primaryArtifactSizeBytes(),
            record.primaryArtifactChecksum(),
            record.status() == PipelineReleaseStatus.ACTIVE
                ? PipelineBundleStatus.ACTIVE
                : PipelineBundleStatus.REGISTERED,
            record.manifest(),
            record.createdAtEpochMs(),
            record.updatedAtEpochMs(),
            record.activatedAtEpochMs());
    }

    private static Path localPath(String uri) {
        if (uri.startsWith("file:")) {
            return Path.of(java.net.URI.create(uri)).toAbsolutePath().normalize();
        }
        return Path.of(uri).toAbsolutePath().normalize();
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to read artifact size: " + e.getMessage(), e);
        }
    }

    private static String sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream stream = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = stream.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                    }
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest algorithm is unavailable", e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to compute artifact digest: " + e.getMessage(), e);
        }
    }

    private PipelineReleaseDescriptorLoader loader() {
        return descriptorLoader == null ? new PipelineReleaseDescriptorLoader() : descriptorLoader;
    }

    private PipelineBundleManifestLoader manifestLoader() {
        return manifestLoader == null ? new PipelineBundleManifestLoader() : manifestLoader;
    }

    private PipelineBundleArtifactStore artifactStore() {
        return artifactStore == null
            ? new LocalPipelineBundleArtifactStore(PipelineBundleRuntimeBeans.storageRoot(orchestratorConfig), manifestLoader())
            : artifactStore;
    }

    private static void validateRequired(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
    }

    private record ValidatedArtifact(
        String artifactPath,
        long artifactSizeBytes,
        String artifactChecksum,
        String bundleVersionId,
        String bundleHash,
        Optional<PipelineBundleManifest> manifest
    ) {
        static ValidatedArtifact remote() {
            return new ValidatedArtifact("", 0L, "", "", "", Optional.empty());
        }
    }
}
