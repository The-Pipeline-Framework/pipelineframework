package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PipelineBundleRegistryTest {

    private final InMemoryPipelineBundleRegistry registry = new InMemoryPipelineBundleRegistry();

    @TempDir
    Path tempDir;

    @Test
    void registersListsGetsAndActivatesBundles() {
        PipelineBundleRecord v1 = record("sha256:v1", "v1");
        PipelineBundleRecord v2 = record("sha256:v2", "v2");

        assertEquals(v1, registry.register(v1).await().indefinitely());
        assertEquals(v2, registry.register(v2).await().indefinitely());

        assertEquals(List.of(v1, v2), registry.list("tenant-1", "org.example.pipeline").await().indefinitely());
        assertTrue(registry.get("tenant-1", "org.example.pipeline", "sha256:v1").await().indefinitely().isPresent());
        assertTrue(registry.active("tenant-1", "org.example.pipeline").await().indefinitely().isEmpty());

        PipelineBundleRecord activeV1 = registry.activate(
            "tenant-1",
            "org.example.pipeline",
            "sha256:v1",
            10L).await().indefinitely().orElseThrow();
        assertEquals(PipelineBundleStatus.ACTIVE, activeV1.status());

        PipelineBundleRecord activeV2 = registry.activate(
            "tenant-1",
            "org.example.pipeline",
            "sha256:v2",
            20L).await().indefinitely().orElseThrow();
        assertEquals(PipelineBundleStatus.ACTIVE, activeV2.status());
        assertEquals("sha256:v2", registry.active("tenant-1", "org.example.pipeline")
            .await().indefinitely().orElseThrow().bundleVersionId());
        assertEquals(PipelineBundleStatus.REGISTERED, registry.get("tenant-1", "org.example.pipeline", "sha256:v1")
            .await().indefinitely().orElseThrow().status());
    }

    @Test
    void duplicateRegistrationIsIdempotentOnlyForSameMetadata() {
        PipelineBundleRecord first = record("sha256:v1", "v1");
        PipelineBundleRecord same = record("sha256:v1", "v1");
        PipelineBundleRecord conflicting = new PipelineBundleRecord(
            first.tenantId(),
            first.pipelineId(),
            first.bundleVersionId(),
            "other",
            first.artifactPath(),
            PipelineBundleStatus.REGISTERED,
            manifest("sha256:v1", "v1"),
            1L,
            1L,
            0L);

        assertEquals(first, registry.register(first).await().indefinitely());
        assertEquals(first, registry.register(same).await().indefinitely());
        assertThrows(IllegalStateException.class, () -> registry.register(conflicting).await().indefinitely());
    }

    @Test
    void fileBackedRegistryPersistsRecordsAndActiveBundle() {
        FileBackedPipelineBundleRegistry first = new FileBackedPipelineBundleRegistry(tempDir);
        PipelineBundleRecord v1 = record("sha256:v1", "v1");
        PipelineBundleRecord v2 = record("sha256:v2", "v2");

        first.register(v1).await().indefinitely();
        first.register(v2).await().indefinitely();
        first.activate("tenant-1", "org.example.pipeline", "sha256:v2", 20L).await().indefinitely();

        FileBackedPipelineBundleRegistry second = new FileBackedPipelineBundleRegistry(tempDir);

        assertEquals(List.of(v1, v2.withStatus(PipelineBundleStatus.ACTIVE, 20L)),
            second.list("tenant-1", "org.example.pipeline").await().indefinitely());
        assertEquals("sha256:v2", second.active("tenant-1", "org.example.pipeline")
            .await().indefinitely().orElseThrow().bundleVersionId());
    }

    private static PipelineBundleRecord record(String bundleVersionId, String hash) {
        return new PipelineBundleRecord(
            "tenant-1",
            "org.example.pipeline",
            bundleVersionId,
            hash,
            "/tmp/" + hash + ".jar",
            100L,
            "sha256:artifact-" + hash,
            PipelineBundleStatus.REGISTERED,
            manifest(bundleVersionId, hash),
            1L,
            1L,
            0L);
    }

    private static PipelineBundleManifest manifest(String bundleVersionId, String hash) {
        return new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            "org.example.pipeline",
            bundleVersionId,
            hash,
            "COMPUTE",
            "REST",
            "module",
            false,
            "monolith",
            List.of(new PipelineBundleStepDescriptor(
                0,
                "Step",
                "service",
                "ONE_TO_ONE",
                "Input",
                "Output",
                "Runtime",
                "Client",
                null)),
            PipelineBundleCapabilities.defaults());
    }
}
