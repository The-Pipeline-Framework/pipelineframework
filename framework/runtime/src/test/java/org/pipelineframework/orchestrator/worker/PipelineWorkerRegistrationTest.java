package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineWorkerRegistrationTest {

    @Test
    void constructorAcceptsValidRegistration() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "rest", "http://localhost", "artifact-1", "sha256:abc");

        assertEquals("tenant-1", reg.tenantId());
        assertEquals("org.example", reg.pipelineId());
        assertEquals("cv1", reg.contractVersion());
        assertEquals("rv1", reg.releaseVersion());
        assertEquals("worker-1", reg.workerId());
        assertEquals("rest", reg.protocol());
        assertEquals("http://localhost", reg.endpoint());
        assertEquals("artifact-1", reg.artifactId());
        assertEquals("sha256:abc", reg.artifactDigest());
    }

    @Test
    void constructorNormalizesProtocolToLowercase() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "REST", "http://localhost", "", "");

        assertEquals("rest", reg.protocol());
    }

    @Test
    void constructorNormalizesMixedCaseProtocol() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "gRPC", "http://localhost", "", "");

        assertEquals("grpc", reg.protocol());
    }

    @Test
    void constructorTrimsWhitespaceFromRequiredFields() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            " tenant-1 ", " org.example ", " cv1 ", " rv1 ", " worker-1 ",
            " rest ", "http://localhost", "", "");

        assertEquals("tenant-1", reg.tenantId());
        assertEquals("org.example", reg.pipelineId());
        assertEquals("cv1", reg.contractVersion());
        assertEquals("rv1", reg.releaseVersion());
        assertEquals("worker-1", reg.workerId());
        assertEquals("rest", reg.protocol());
    }

    @Test
    void constructorTrimsOptionalFields() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "rest", "  http://localhost  ", "  artifact-1  ", "  sha256:abc  ");

        assertEquals("http://localhost", reg.endpoint());
        assertEquals("artifact-1", reg.artifactId());
        assertEquals("sha256:abc", reg.artifactDigest());
    }

    @Test
    void constructorNullEndpointBecomesEmptyString() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "cv1", "rv1", "worker-1",
            "rest", null, null, null);

        assertEquals("", reg.endpoint());
        assertEquals("", reg.artifactId());
        assertEquals("", reg.artifactDigest());
    }

    @Test
    void constructorRejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "", "org.example", "cv1", "rv1", "worker-1",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankPipelineId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "   ", "cv1", "rv1", "worker-1",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankContractVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "org.example", "", "rv1", "worker-1",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankReleaseVersion() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "org.example", "cv1", "   ", "worker-1",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankWorkerId() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "org.example", "cv1", "rv1", "",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankProtocol() {
        assertThrows(IllegalArgumentException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "org.example", "cv1", "rv1", "worker-1",
                "   ", "", "", ""));
    }

    @Test
    void constructorRejectsNullTenantId() {
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRegistration(
                null, "org.example", "cv1", "rv1", "worker-1",
                "rest", "", "", ""));
    }

    @Test
    void constructorRejectsNullProtocol() {
        assertThrows(NullPointerException.class, () ->
            new PipelineWorkerRegistration(
                "tenant-1", "org.example", "cv1", "rv1", "worker-1",
                null, "", "", ""));
    }
}