package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;

class PipelineReleaseRecordTest {

    @Test
    void constructsWithAllRequiredFields() {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseRecord record = sampleRecord("file:///opt/tpf/app.jar", 1024L, "sha256:abc");

        assertEquals("tenant-1", record.tenantId());
        assertEquals(contract.pipelineId(), record.pipelineId());
        assertEquals(contract.contractVersion(), record.contractVersion());
        assertEquals("release-1", record.releaseVersion());
        assertEquals(PipelineReleaseStatus.REGISTERED, record.status());
        assertEquals("file:///opt/tpf/app.jar", record.primaryArtifactUri());
        assertEquals(1024L, record.primaryArtifactSizeBytes());
        assertEquals("sha256:abc", record.primaryArtifactChecksum());
    }

    @Test
    void normalizesNullPrimaryArtifactUriToEmpty() {
        PipelineReleaseRecord record = sampleRecord(null, 0L, "sha256:abc");

        assertEquals("", record.primaryArtifactUri());
    }

    @Test
    void normalizesNullPrimaryArtifactChecksumToEmpty() {
        PipelineReleaseRecord record = sampleRecord("file:///opt/tpf/app.jar", 0L, null);

        assertEquals("", record.primaryArtifactChecksum());
    }

    @Test
    void normalizesNullPrimaryArtifactIdToEmpty() {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseDescriptor descriptor = descriptor(contract, "release-1");
        PipelineReleaseRecord record = new PipelineReleaseRecord(
            "tenant-1", contract.pipelineId(), contract.contractVersion(), "release-1",
            PipelineReleaseStatus.REGISTERED, descriptor,
            null, null, "file:///opt/tpf/app.jar", 1024L, "sha256:abc",
            contract, 1000L, 1000L, 0L);

        assertEquals("", record.primaryArtifactId());
        assertEquals("", record.primaryArtifactDigest());
    }

    @Test
    void rejectsNullTenantId() {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseDescriptor descriptor = descriptor(contract, "release-1");
        assertThrows(NullPointerException.class, () ->
            new PipelineReleaseRecord(
                null, contract.pipelineId(), contract.contractVersion(), "release-1",
                PipelineReleaseStatus.REGISTERED, descriptor,
                "app", "sha256:abc", "file:///opt/tpf/app.jar", 1024L, "sha256:abc",
                contract, 1000L, 1000L, 0L));
    }

    @Test
    void rejectsNullStatus() {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseDescriptor descriptor = descriptor(contract, "release-1");
        assertThrows(NullPointerException.class, () ->
            new PipelineReleaseRecord(
                "tenant-1", contract.pipelineId(), contract.contractVersion(), "release-1",
                null, descriptor,
                "app", "sha256:abc", "file:///opt/tpf/app.jar", 1024L, "sha256:abc",
                contract, 1000L, 1000L, 0L));
    }

    @Test
    void rejectsNullDescriptor() {
        PipelineContractDescriptor contract = contract();
        assertThrows(NullPointerException.class, () ->
            new PipelineReleaseRecord(
                "tenant-1", contract.pipelineId(), contract.contractVersion(), "release-1",
                PipelineReleaseStatus.REGISTERED, null,
                "app", "sha256:abc", "file:///opt/tpf/app.jar", 1024L, "sha256:abc",
                contract, 1000L, 1000L, 0L));
    }

    @Test
    void withStatusPreservesPrimaryArtifactUri() {
        PipelineReleaseRecord original = sampleRecord("s3://bucket/releases/sha256/abc.jar", 2048L, "sha256:abc");

        PipelineReleaseRecord activated = original.withStatus(PipelineReleaseStatus.ACTIVE, 2000L);

        assertEquals("s3://bucket/releases/sha256/abc.jar", activated.primaryArtifactUri());
        assertEquals(2048L, activated.primaryArtifactSizeBytes());
        assertEquals("sha256:abc", activated.primaryArtifactChecksum());
    }

    @Test
    void withStatusSetsActivatedAtForActiveStatus() {
        PipelineReleaseRecord original = sampleRecord("file:///opt/tpf/app.jar", 1024L, "sha256:abc");

        PipelineReleaseRecord activated = original.withStatus(PipelineReleaseStatus.ACTIVE, 5000L);

        assertEquals(PipelineReleaseStatus.ACTIVE, activated.status());
        assertEquals(5000L, activated.activatedAtEpochMs());
        assertEquals(5000L, activated.updatedAtEpochMs());
        assertEquals(original.createdAtEpochMs(), activated.createdAtEpochMs());
    }

    @Test
    void withStatusPreservesActivatedAtForNonActiveStatus() {
        PipelineReleaseRecord original = sampleRecord("file:///opt/tpf/app.jar", 1024L, "sha256:abc");
        PipelineReleaseRecord activated = original.withStatus(PipelineReleaseStatus.ACTIVE, 5000L);

        PipelineReleaseRecord deprecated = activated.withStatus(PipelineReleaseStatus.REGISTERED, 6000L);

        assertEquals(5000L, deprecated.activatedAtEpochMs());
        assertEquals(6000L, deprecated.updatedAtEpochMs());
    }

    @Test
    void withStatusPreservesFileUriAcrossTransitions() {
        String fileUri = "file:///var/lib/tpf/releases/artifacts/sha256/deadbeef.jar";
        PipelineReleaseRecord original = sampleRecord(fileUri, 512L, "sha256:deadbeef");

        PipelineReleaseRecord result = original
            .withStatus(PipelineReleaseStatus.ACTIVE, 2000L)
            .withStatus(PipelineReleaseStatus.REGISTERED, 3000L);

        assertEquals(fileUri, result.primaryArtifactUri());
    }

    @Test
    void withStatusPreservesS3UriAcrossTransitions() {
        String s3Uri = "s3://tpf-release-artifacts/tpf/releases/sha256/cafebabe.zip";
        PipelineReleaseRecord original = sampleRecord(s3Uri, 4096L, "sha256:cafebabe");

        PipelineReleaseRecord result = original.withStatus(PipelineReleaseStatus.ACTIVE, 2000L);

        assertEquals(s3Uri, result.primaryArtifactUri());
    }

    // --- helpers ---

    private static PipelineReleaseRecord sampleRecord(String uri, long size, String checksum) {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseDescriptor descriptor = descriptor(contract, "release-1");
        return new PipelineReleaseRecord(
            "tenant-1",
            contract.pipelineId(),
            contract.contractVersion(),
            "release-1",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            "app",
            "sha256:artifact-digest",
            uri,
            size,
            checksum,
            contract,
            1000L,
            1000L,
            0L);
    }

    private static PipelineContractDescriptor contract() {
        return PipelineContractDescriptor.localFallback();
    }

    private static PipelineReleaseDescriptor descriptor(PipelineContractDescriptor contract, String releaseVersion) {
        PipelineReleaseArtifactDescriptor artifact = new PipelineReleaseArtifactDescriptor(
            "app", "jar", "file:///opt/tpf/app.jar", "sha256:artifact-digest", List.of(), List.of());
        return new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(),
            contract.contractVersion(),
            releaseVersion,
            List.of(artifact));
    }
}
