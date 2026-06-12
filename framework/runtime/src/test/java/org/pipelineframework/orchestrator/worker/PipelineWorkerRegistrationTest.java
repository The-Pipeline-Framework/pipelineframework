package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class PipelineWorkerRegistrationTest {

    @Test
    void constructorNormalizesProtocolToLowerCase() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", "worker-1", "REST",
            "http://localhost", "artifact-1", "sha256:d1");

        assertEquals("rest", reg.protocol());
    }

    @Test
    void constructorNormalizesNullOptionalFieldsToEmpty() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", "worker-1", "local",
            null, null, null);

        assertEquals("", reg.endpoint());
        assertEquals("", reg.artifactId());
        assertEquals("", reg.artifactDigest());
    }

    @Test
    void constructorTrimsWhitespaceFromRequiredFields() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "  tenant-1  ", "  org.example  ", "  v1  ", "  r1  ", "  worker-1  ",
            "  rest  ", "  http://localhost  ", "  artifact-1  ", "  sha256:d1  ");

        assertEquals("tenant-1", reg.tenantId());
        assertEquals("org.example", reg.pipelineId());
        assertEquals("v1", reg.contractVersion());
        assertEquals("r1", reg.releaseVersion());
        assertEquals("worker-1", reg.workerId());
        assertEquals("rest", reg.protocol());
        assertEquals("http://localhost", reg.endpoint());
        assertEquals("artifact-1", reg.artifactId());
        assertEquals("sha256:d1", reg.artifactDigest());
    }

    @Test
    void constructorRejectsNullTenantId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRegistration(
            null, "org.example", "v1", "r1", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankTenantId() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "  ", "org.example", "v1", "r1", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsNullPipelineId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", null, "v1", "r1", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankPipelineId() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "", "v1", "r1", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankContractVersion() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "   ", "r1", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankReleaseVersion() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "", "worker-1", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsNullWorkerId() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", null, "rest", "", "", ""));
    }

    @Test
    void constructorRejectsBlankWorkerId() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", "  ", "rest", "", "", ""));
    }

    @Test
    void constructorRejectsNullProtocol() {
        assertThrows(NullPointerException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", "worker-1", null, "", "", ""));
    }

    @Test
    void constructorRejectsBlankProtocol() {
        assertThrows(IllegalArgumentException.class, () -> new PipelineWorkerRegistration(
            "tenant-1", "org.example", "v1", "r1", "worker-1", "", "", "", ""));
    }

    @Test
    void constructorAcceptsAllValidFields() {
        PipelineWorkerRegistration reg = new PipelineWorkerRegistration(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:release",
            "rest-worker",
            "grpc",
            "http://worker:9090",
            "restaurant-artifact",
            "sha256:digest");

        assertEquals("tenant-1", reg.tenantId());
        assertEquals("org.example.restaurant", reg.pipelineId());
        assertEquals("sha256:contract", reg.contractVersion());
        assertEquals("sha256:release", reg.releaseVersion());
        assertEquals("rest-worker", reg.workerId());
        assertEquals("grpc", reg.protocol());
        assertEquals("http://worker:9090", reg.endpoint());
        assertEquals("restaurant-artifact", reg.artifactId());
        assertEquals("sha256:digest", reg.artifactDigest());
    }
}