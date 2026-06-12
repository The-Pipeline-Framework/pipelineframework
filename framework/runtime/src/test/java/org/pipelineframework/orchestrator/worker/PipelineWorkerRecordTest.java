package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PipelineWorkerRecordTest {

    @Test
    void constructorNormalizesNullEndpointArtifactIdAndArtifactDigestToEmpty() {
        PipelineWorkerRecord record = new PipelineWorkerRecord(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "rest", null, null, null,
            PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L);

        assertEquals("", record.endpoint());
        assertEquals("", record.artifactId());
        assertEquals("", record.artifactDigest());
    }

    @Test
    void constructorRejectsNullRequiredFields() {
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord(null, "org.example", "cv1", "rv1", "worker-1",
                "rest", "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", null, "cv1", "rv1", "worker-1",
                "rest", "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", "org.example", null, "rv1", "worker-1",
                "rest", "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", "org.example", "cv1", null, "worker-1",
                "rest", "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", "org.example", "cv1", "rv1", null,
                "rest", "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", "org.example", "cv1", "rv1", "worker-1",
                null, "", "", "", PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L));
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRecord("tenant-1", "org.example", "cv1", "rv1", "worker-1",
                "rest", "", "", "", null, 1_000L, 1_000L, 0L));
    }

    @Test
    void matchesReturnsTrueWhenAllFieldsAlignWithBothArtifactsBlank() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv1", "", "");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsTrueWhenArtifactIdMatchesAndDigestBlankOnRequest() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-1", "");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsTrueWhenArtifactIdBlankOnRecord() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-1", "sha256:abc");

        assertTrue(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenArtifactIdMismatchesAndBothNonBlank() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-2", "sha256:abc");

        assertFalse(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenArtifactDigestMismatchesAndBothNonBlank() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-1", "sha256:xyz");

        assertFalse(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenReleaseMismatches() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv2", "", "");

        assertFalse(record.matches(request, "rest"));
    }

    @Test
    void matchesReturnsFalseWhenProtocolMismatches() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv1", "", "");

        assertFalse(record.matches(request, "grpc"));
    }

    @Test
    void matchesIsCaseInsensitiveForProtocol() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv1", "", "");

        assertTrue(record.matches(request, "REST"));
        assertTrue(record.matches(request, "Rest"));
    }

    @Test
    void hasArtifactMismatchReturnsTrueForDifferentArtifactIds() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-2", "sha256:abc");

        assertTrue(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsTrueForDifferentArtifactDigests() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-1", "sha256:xyz");

        assertTrue(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenArtifactIdBlankOnRecord() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-1", "sha256:abc");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenArtifactIdBlankOnRequest() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "", "sha256:abc");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenBothArtifactsBlank() {
        PipelineWorkerRecord record = record("rest", "rv1", "", "");
        PipelineWorkerAvailabilityRequest request = request("rv1", "", "");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenProviderMismatches() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv1", "artifact-2", "sha256:xyz");

        assertFalse(record.hasArtifactMismatch(request, "grpc"));
    }

    @Test
    void hasArtifactMismatchReturnsFalseWhenReleaseMismatches() {
        PipelineWorkerRecord record = record("rest", "rv1", "artifact-1", "sha256:abc");
        PipelineWorkerAvailabilityRequest request = request("rv2", "artifact-2", "sha256:xyz");

        assertFalse(record.hasArtifactMismatch(request, "rest"));
    }

    @Test
    void sameReleaseAndProviderMatchesCrossFieldsCorrectly() {
        PipelineWorkerRecord record = record("grpc", "rv1", "", "");
        PipelineWorkerAvailabilityRequest matching = request("rv1", "", "");
        PipelineWorkerAvailabilityRequest differentRelease = request("rv2", "", "");

        assertTrue(record.sameReleaseAndProvider(matching, "grpc"));
        assertFalse(record.sameReleaseAndProvider(differentRelease, "grpc"));
        assertFalse(record.sameReleaseAndProvider(matching, "rest"));
    }

    @Test
    void sameReleaseAndProviderMatchesDifferentTenant() {
        PipelineWorkerRecord record = new PipelineWorkerRecord(
            "tenant-A", "org.example", "cv1", "rv1", "worker-1",
            "rest", "", "", "",
            PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L);
        PipelineWorkerAvailabilityRequest requestForTenantB = new PipelineWorkerAvailabilityRequest(
            "tenant-B", "org.example", "cv1", "rv1", "", "");

        assertFalse(record.sameReleaseAndProvider(requestForTenantB, "rest"));
    }

    private static PipelineWorkerRecord record(
        String protocol,
        String releaseVersion,
        String artifactId,
        String artifactDigest) {
        return new PipelineWorkerRecord(
            "tenant-1", "org.example", "cv1", releaseVersion, "worker-1",
            protocol, "http://localhost", artifactId, artifactDigest,
            PipelineWorkerState.HEALTHY, 1_000L, 1_000L, 0L);
    }

    private static PipelineWorkerAvailabilityRequest request(
        String releaseVersion,
        String artifactId,
        String artifactDigest) {
        return new PipelineWorkerAvailabilityRequest(
            "tenant-1", "org.example", "cv1", releaseVersion, artifactId, artifactDigest);
    }
}