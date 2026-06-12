package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineReleaseStoredArtifactTest {

    @Test
    void constructsWithValidArguments() {
        PipelineReleaseStoredArtifact artifact = new PipelineReleaseStoredArtifact(
            "file:///var/lib/tpf/releases/artifacts/abc123.jar", 1024L, "sha256:abc123");

        assertEquals("file:///var/lib/tpf/releases/artifacts/abc123.jar", artifact.artifactUri());
        assertEquals(1024L, artifact.artifactSizeBytes());
        assertEquals("sha256:abc123", artifact.artifactChecksum());
    }

    @Test
    void constructsWithS3Uri() {
        PipelineReleaseStoredArtifact artifact = new PipelineReleaseStoredArtifact(
            "s3://tpf-release-artifacts/tpf/releases/sha256/abc123.jar", 2048L, "sha256:abc123");

        assertEquals("s3://tpf-release-artifacts/tpf/releases/sha256/abc123.jar", artifact.artifactUri());
        assertEquals(2048L, artifact.artifactSizeBytes());
    }

    @Test
    void constructsWithZeroSize() {
        PipelineReleaseStoredArtifact artifact = new PipelineReleaseStoredArtifact(
            "file:///path/to/empty.jar", 0L, "sha256:e3b0");

        assertEquals(0L, artifact.artifactSizeBytes());
    }

    @Test
    void rejectsNullArtifactUri() {
        assertThrows(NullPointerException.class, () ->
            new PipelineReleaseStoredArtifact(null, 100L, "sha256:abc"));
    }

    @Test
    void rejectsNullArtifactChecksum() {
        assertThrows(NullPointerException.class, () ->
            new PipelineReleaseStoredArtifact("file:///path/to/app.jar", 100L, null));
    }

    @Test
    void rejectsNegativeArtifactSize() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineReleaseStoredArtifact("file:///path/to/app.jar", -1L, "sha256:abc"));
    }

    @Test
    void rejectsNegativeLargeArtifactSize() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineReleaseStoredArtifact("s3://bucket/key", Long.MIN_VALUE, "sha256:abc"));
    }

    @Test
    void artifactUriFieldReplacedArtifactPath() {
        // Regression: field was renamed from artifactPath to artifactUri.
        // Constructing with the new field name must succeed and return the URI.
        String uri = "file:///opt/tpf/artifacts/sha256/cafebabe.jar";
        PipelineReleaseStoredArtifact artifact = new PipelineReleaseStoredArtifact(uri, 512L, "sha256:cafebabe");

        assertEquals(uri, artifact.artifactUri());
    }
}