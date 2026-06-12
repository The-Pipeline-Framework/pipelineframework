package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PipelineWorkerRecordTest {

    @Test
    void constructorNormalizesNullOptionalFieldsToEmpty() {
        PipelineWorkerRecord record = new PipelineWorkerRecord(
            "tenant-1",
            "org.example",
            "v1",
            "r1",
            "worker-1",
            "rest",
            null,
            null,
            null,
            PipelineWorkerState.HEALTHY,
            1_000L,
            1_000L,
            0L);

        assertEquals("", record.endpoint());
        assertEquals("", record.artifactId());
        assertEquals("", record.artifactDigest());
    }

    @Test
    void constructorRejectsNullTenantId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRecord(
            null, "org.example", "v1", "r1", "worker-1", "rest", "",
            "", "", PipelineWorkerState.HEALTHY, 0L, 0L, 0L));
    }

    @Test
    void constructorRejectsNullPipelineId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRecord(
            "t1", null, "v1", "r1", "worker-1", "rest", "",
            "", "", PipelineWorkerState.HEALTHY, 0L, 0L, 0L));
    }

    @Test
    void constructorRejectsNullWorkerId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRecord(
            "t1", "org.example", "v1", "r1", null, "rest", "",
            "", "", PipelineWorkerState.HEALTHY, 0L, 0L, 0L));
    }

    @Test
    void constructorRejectsNullState() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRecord(
            "t1", "org.example", "v1", "r1", "worker-1", "rest", "",
            "", "", null, 0L, 0L, 0L));
    }

    @Test
    void sameReleaseAndProviderMatchesCaseInsensitiveProtocol() {
        PipelineWorkerRecord record = record("REST", "sha256:contract", "sha256:release");
        PipelineWorkerAvailabilityRequest request = request("sha256:contract", "sha256:release");

        assertTrue(record.sameReleaseAndProvider(request, "rest"));
        assertTrue(record.sameReleaseAndProvider(request, "REST"));
        assertTrue(record.sameReleaseAndProvider(request, "Rest"));
    }

    @Test
    void sameReleaseAndProviderRejectsDifferentProtocol() {
        PipelineWorkerRecord record = record("rest", "sha256:contract", "sha256:release");
        PipelineWorkerAvailabilityRequest request = request("sha256:contract", "sha256:release");

        assertFalse(record.sameReleaseAndProvider(request, "grpc"));
    }

    @Test
    void sameReleaseAndProviderRejectsDifferentReleaseVersion() {
        PipelineWorkerRecord record = record("rest", "sha256:contract", "sha256:release-1");
        PipelineWorkerAvailabilityRequest request = request("sha256:contract", "sha256:release-2");

        assertFalse(record.sameReleaseAndProvider(request, "rest"));
    }

    @Test
    void sameReleaseAndProviderRejectsDifferentContractVersion() {
        PipelineWorkerRecord record = record("rest", "sha256:contract-1", "sha256:release");
        PipelineWorkerAvailabilityRequest request = request("sha256:contract-2", "sha256:release");

        assertFalse(record.sameReleaseAndProvider(request, "rest"));
    }

    @Test
    void matchesReturnsTrueWhenArtifactIdsMatchExactly() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-1", "sha256:d1");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsTrueWhenRequestArtifactIdIsBlank() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "", "");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsTrueWhenRecordArtifactIdIsBlank() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "", "");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-1", "sha256:d1");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenArtifactIdsConflict() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-2", "");

        assertFalse(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenArtifactDigestsConflict() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "", "sha256:d2");

        assertFalse(record.matches(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenArtifactIdsMatch() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-1", "sha256:d1");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsTrueWhenArtifactIdsDiffer() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-2", "");

        assertTrue(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsTrueWhenDigestsDiffer() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-1", "sha256:d2");

        assertTrue(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenRequestArtifactIsBlank() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "", "");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenRecordArtifactIsBlank() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "", "");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-1", "sha256:d1");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenProviderDoesNotMatch() {
        PipelineWorkerRecord record = recordWithArtifact("rest", "sha256:release", "artifact-1", "sha256:d1");
        PipelineWorkerAvailabilityRequest request = requestWithArtifact("sha256:release", "artifact-2", "sha256:d2");

        assertFalse(record.hasArtifactMismatch(request, "grpc"));
    }

    private static PipelineWorkerRecord record(
        String protocol,
        String contractVersion,
        String releaseVersion) {
        return new PipelineWorkerRecord(
            "tenant-1",
            "org.example",
            contractVersion,
            releaseVersion,
            "worker-1",
            protocol,
            "http://localhost",
            "",
            "",
            PipelineWorkerState.HEALTHY,
            1_000L,
            1_000L,
            0L);
    }

    private static PipelineWorkerRecord recordWithArtifact(
        String protocol,
        String releaseVersion,
        String artifactId,
        String artifactDigest) {
        return new PipelineWorkerRecord(
            "tenant-1",
            "org.example",
            "sha256:contract",
            releaseVersion,
            "worker-1",
            protocol,
            "http://localhost",
            artifactId,
            artifactDigest,
            PipelineWorkerState.HEALTHY,
            1_000L,
            1_000L,
            0L);
    }

    private static PipelineWorkerAvailabilityRequest request(
        String contractVersion,
        String releaseVersion) {
        return new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example",
            contractVersion,
            releaseVersion,
            "",
            "");
    }

    private static PipelineWorkerAvailabilityRequest requestWithArtifact(
        String releaseVersion,
        String artifactId,
        String artifactDigest) {
        return new PipelineWorkerAvailabilityRequest(
            "tenant-1",
            "org.example",
            "sha256:contract",
            releaseVersion,
            artifactId,
            artifactDigest);
    }
}