package org.pipelineframework.orchestrator.release;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineReleaseContractTest {

    @Test
    void descriptorSnapshotsArtifactCollections() {
        List<String> stepIds = new ArrayList<>(List.of("charge"));
        List<PipelineReleaseArtifactDescriptor> artifacts = new ArrayList<>(List.of(
            new PipelineReleaseArtifactDescriptor(
                "payments", "application", "maven:example:payments:1", "sha256:artifact", stepIds, List.of("grpc"))));

        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            "payments",
            "contract-v1",
            "release-v1",
            artifacts);

        stepIds.add("receipt");
        artifacts.clear();

        assertEquals(List.of("charge"), descriptor.artifacts().getFirst().stepIds());
        assertThrows(UnsupportedOperationException.class, () -> descriptor.artifacts().add(
            new PipelineReleaseArtifactDescriptor("other", "application", "uri", "digest", List.of(), List.of())));
    }

    @Test
    void activationChangesOnlyReleaseLifecycleState() {
        PipelineReleaseDescriptor descriptor = new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            "payments",
            "contract-v1",
            "release-v1",
            List.of());
        PipelineReleaseRecord registered = new PipelineReleaseRecord(
            "tenant",
            "payments",
            "contract-v1",
            "release-v1",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            "payments",
            "sha256:artifact",
            "maven:example:payments:1",
            42,
            "sha256:content",
            PipelineContractDescriptor.localFallback(),
            10,
            10,
            0);

        PipelineReleaseRecord active = registered.withStatus(PipelineReleaseStatus.ACTIVE, 20);

        assertEquals(PipelineReleaseStatus.ACTIVE, active.status());
        assertEquals(20, active.updatedAtEpochMs());
        assertEquals(20, active.activatedAtEpochMs());
        assertEquals(registered.descriptor(), active.descriptor());
        assertEquals(registered.contract(), active.contract());
    }

    @Test
    void rejectsUnknownReleaseSchema() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION + 1,
            "payments",
            "contract-v1",
            "release-v1",
            List.of()));
    }
}
