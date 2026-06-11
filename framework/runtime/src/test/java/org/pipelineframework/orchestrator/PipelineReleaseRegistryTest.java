package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;

class PipelineReleaseRegistryTest {

    @TempDir
    Path tempDir;

    @Test
    void registrarValidatesJarReleaseAndStoresExecutableArtifact() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path jar = jar(manifest);
        Path descriptor = releaseDescriptor(jar, manifest, "sha256:" + sha256(jar));
        PipelineReleaseRegistrar registrar = registrar();

        PipelineReleaseRecord record = registrar.validate(
            "tenant-1",
            "org.example.restaurant",
            descriptor.toString(),
            1000L);

        assertEquals("sha256:bundle", record.releaseVersion());
        assertEquals("sha256:bundle", record.contractVersion());
        assertEquals("sha256:bundle", record.bundleVersionId());
        assertTrue(Files.isRegularFile(Path.of(record.primaryArtifactPath())));
        registrar.verify(record);
    }

    @Test
    void registrarRejectsMismatchedArtifactDigest() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path jar = jar(manifest);
        Path descriptor = releaseDescriptor(jar, manifest, "sha256:deadbeef");
        PipelineReleaseRegistrar registrar = registrar();

        assertThrows(IllegalArgumentException.class, () -> registrar.validate(
            "tenant-1",
            "org.example.restaurant",
            descriptor.toString(),
            1000L));
    }

    @Test
    void fileRegistryPersistsRegisteredAndActiveRelease() throws Exception {
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new FileBackedPipelineReleaseRegistry(tempDir);

        registry.register(record).await().indefinitely();
        registry.activate("tenant-1", "org.example.restaurant", "sha256:bundle", 2000L)
            .await().indefinitely();

        PipelineReleaseRegistry reloaded = new FileBackedPipelineReleaseRegistry(tempDir);
        PipelineReleaseRecord active = reloaded.active("tenant-1", "org.example.restaurant")
            .await().indefinitely()
            .orElseThrow();

        assertEquals("sha256:bundle", active.releaseVersion());
        assertEquals(PipelineReleaseStatus.ACTIVE, active.status());
        assertEquals(2000L, active.activatedAtEpochMs());
    }

    private PipelineReleaseRegistrar registrar() {
        PipelineReleaseRegistrar registrar = new PipelineReleaseRegistrar();
        registrar.descriptorLoader = new PipelineReleaseDescriptorLoader();
        registrar.manifestLoader = new PipelineBundleManifestLoader();
        registrar.artifactStore = new LocalPipelineBundleArtifactStore(tempDir.resolve("store"), registrar.manifestLoader);
        return registrar;
    }

    private PipelineReleaseRecord releaseRecord() {
        PipelineBundleManifest manifest = manifest();
        PipelineReleaseDescriptor descriptor = descriptor("/tmp/restaurant.jar", "sha256:artifact", manifest);
        return new PipelineReleaseRecord(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "sha256:bundle",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            "sha256:bundle",
            "bundle",
            "/tmp/restaurant.jar",
            100L,
            "artifact",
            manifest,
            1000L,
            1000L,
            0L);
    }

    private Path releaseDescriptor(Path jar, PipelineBundleManifest manifest, String digest) throws Exception {
        Path descriptorPath = tempDir.resolve("pipeline-release.json");
        PipelineJson.mapper().writerWithDefaultPrettyPrinter()
            .writeValue(descriptorPath.toFile(), descriptor(jar.toString(), digest, manifest));
        return descriptorPath;
    }

    private PipelineReleaseDescriptor descriptor(String uri, String digest, PipelineBundleManifest manifest) {
        return new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            manifest.pipelineId(),
            "sha256:" + manifest.bundleHash(),
            manifest.bundleVersionId(),
            List.of(new PipelineReleaseArtifactDescriptor(
                "restaurant",
                "jar",
                uri,
                digest,
                manifest.bundleVersionId(),
                manifest.bundleHash(),
                List.of("Validate"),
                List.of("rest"))));
    }

    private Path jar(PipelineBundleManifest manifest) throws Exception {
        Path jar = tempDir.resolve("restaurant.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(PipelineBundleManifest.RESOURCE_PATH));
            output.write(PipelineJson.mapper().writeValueAsBytes(manifest));
            output.closeEntry();
        }
        return jar;
    }

    private PipelineBundleManifest manifest() {
        return new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            "org.example.restaurant",
            "sha256:bundle",
            "bundle",
            "COMPUTE",
            "REST",
            "monolith-svc",
            false,
            "monolith",
            List.of(new PipelineBundleStepDescriptor(
                0,
                "Validate",
                "service",
                "ONE_TO_ONE",
                String.class.getName(),
                "Output",
                "Runtime",
                "Client",
                null)),
            PipelineBundleCapabilities.defaults());
    }

    private String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(Files.readAllBytes(path));
        return HexFormat.of().formatHex(digest.digest());
    }
}
