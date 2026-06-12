package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.LocalControlPlaneSecretResolver;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.worker.dto.HostedWorkerRegisterRequest;

class HostedWorkerLifecycleAdminResourceTest {

    private static final String TENANT_ID = "tenant-1";
    private static final String PIPELINE_ID = "org.example.restaurant";
    private static final String TOKEN = "admin-token";
    private static final String AUTH = "Bearer " + TOKEN;

    private HostedWorkerLifecycleAdminResource resource;
    private PipelineOrchestratorConfig.AdminConfig adminConfig;

    @BeforeEach
    void setUp() {
        resource = new HostedWorkerLifecycleAdminResource();
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        adminConfig = mock(PipelineOrchestratorConfig.AdminConfig.class);
        resource.orchestratorConfig = config;
        resource.workerRegistry = new InMemoryPipelineWorkerRegistry();
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        when(config.admin()).thenReturn(adminConfig);
        when(adminConfig.enabled()).thenReturn(true);
        when(adminConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());
    }

    @Test
    void registerListHeartbeatAndDrainWorker() {
        Response register = resource.register(TENANT_ID, PIPELINE_ID, AUTH, request())
            .await().indefinitely();

        assertEquals(200, register.getStatus());
        PipelineWorkerRecord registered = assertInstanceOf(PipelineWorkerRecord.class, register.getEntity());
        assertEquals(PipelineWorkerState.HEALTHY, registered.state());

        Response list = resource.list(TENANT_ID, PIPELINE_ID, AUTH, null, null)
            .await().indefinitely();
        assertEquals(200, list.getStatus());
        assertEquals(1, assertInstanceOf(List.class, list.getEntity()).size());

        Response heartbeat = resource.heartbeat(TENANT_ID, PIPELINE_ID, "worker-1", AUTH)
            .await().indefinitely();
        assertEquals(200, heartbeat.getStatus());

        Response drain = resource.markDraining(TENANT_ID, PIPELINE_ID, "worker-1", AUTH)
            .await().indefinitely();
        assertEquals(200, drain.getStatus());
        PipelineWorkerRecord draining = assertInstanceOf(PipelineWorkerRecord.class, drain.getEntity());
        assertEquals(PipelineWorkerState.DRAINING, draining.state());
    }

    @Test
    void rejectsWhenDisabled() {
        when(adminConfig.enabled()).thenReturn(false);

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH, null, null)
            .await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void rejectsInvalidToken() {
        Response response = resource.list(TENANT_ID, PIPELINE_ID, "Bearer wrong", null, null)
            .await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void rejectsInvalidRegistrationRequest() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
                "",
                "sha256:contract",
                "sha256:release",
                "rest",
                "http://localhost",
                "",
                ""))
            .await().indefinitely();

        assertEquals(400, response.getStatus());
    }

    @Test
    void rejectsMissingBearerPrefix() {
        Response response = resource.list(TENANT_ID, PIPELINE_ID, "Basic " + TOKEN, null, null)
            .await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void rejectsEmptyBearerToken() {
        Response response = resource.list(TENANT_ID, PIPELINE_ID, "Bearer ", null, null)
            .await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void rejectsMissingAuthorizationHeader() {
        Response response = resource.list(TENANT_ID, PIPELINE_ID, null, null, null)
            .await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    @Test
    void heartbeatOnNonExistentWorkerReturns404() {
        Response response = resource.heartbeat(TENANT_ID, PIPELINE_ID, "nonexistent", AUTH)
            .await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void drainOnNonExistentWorkerReturns404() {
        Response response = resource.markDraining(TENANT_ID, PIPELINE_ID, "nonexistent", AUTH)
            .await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void listFiltersWorkersByContractVersion() {
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, request()).await().indefinitely();
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-2", "sha256:other-contract", "sha256:release", "rest",
            "http://localhost:8282", "", "")).await().indefinitely();

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH, "sha256:contract", null)
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(1, assertInstanceOf(List.class, response.getEntity()).size());
    }

    @Test
    void listFiltersWorkersByReleaseVersion() {
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, request()).await().indefinitely();
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-2", "sha256:contract", "sha256:other-release", "rest",
            "http://localhost:8282", "", "")).await().indefinitely();

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH, null, "sha256:release")
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(1, assertInstanceOf(List.class, response.getEntity()).size());
    }

    @Test
    void listWithNoFiltersReturnsAllWorkers() {
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, request()).await().indefinitely();
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-2", "sha256:contract", "sha256:release-2", "grpc",
            "http://localhost:9090", "", "")).await().indefinitely();

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH, null, null)
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(2, assertInstanceOf(List.class, response.getEntity()).size());
    }

    @Test
    void rejectsNullRegistrationRequest() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, null)
            .await().indefinitely();

        assertEquals(400, response.getStatus());
    }

    @Test
    void rejectsRegistrationMissingContractVersion() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-1", "", "sha256:release", "rest", "http://localhost", "", ""))
            .await().indefinitely();

        assertEquals(400, response.getStatus());
    }

    @Test
    void rejectsRegistrationMissingReleaseVersion() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-1", "sha256:contract", null, "rest", "http://localhost", "", ""))
            .await().indefinitely();

        assertEquals(400, response.getStatus());
    }

    @Test
    void rejectsRegistrationMissingProtocol() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, new HostedWorkerRegisterRequest(
            "worker-1", "sha256:contract", "sha256:release", "   ", "http://localhost", "", ""))
            .await().indefinitely();

        assertEquals(400, response.getStatus());
    }

    @Test
    void registrationReturnedRecordHasCorrectFields() {
        Response response = resource.register(TENANT_ID, PIPELINE_ID, AUTH, request())
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        PipelineWorkerRecord record = assertInstanceOf(PipelineWorkerRecord.class, response.getEntity());
        assertEquals(TENANT_ID, record.tenantId());
        assertEquals(PIPELINE_ID, record.pipelineId());
        assertEquals("worker-1", record.workerId());
        assertEquals("sha256:contract", record.contractVersion());
        assertEquals("sha256:release", record.releaseVersion());
        assertEquals("rest", record.protocol());
        assertEquals(PipelineWorkerState.HEALTHY, record.state());
    }

    @Test
    void drainedWorkerIsReportedAsDrainingInList() {
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, request()).await().indefinitely();
        resource.markDraining(TENANT_ID, PIPELINE_ID, "worker-1", AUTH).await().indefinitely();

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH, null, null)
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        List<?> workers = assertInstanceOf(List.class, response.getEntity());
        assertEquals(1, workers.size());
        PipelineWorkerRecord record = assertInstanceOf(PipelineWorkerRecord.class, workers.get(0));
        assertEquals(PipelineWorkerState.DRAINING, record.state());
    }

    @Test
    void workerBecomesStaleAfterHeartbeatTtlExpires() {
        InMemoryPipelineWorkerRegistry registry = new InMemoryPipelineWorkerRegistry();
        registry.register(new PipelineWorkerRegistration(
            TENANT_ID, PIPELINE_ID, "sha256:contract", "sha256:release", "worker-stale",
            "rest", "http://localhost:8181", "", ""), 1_000L).await().indefinitely();

        List<PipelineWorkerRecord> staleRecords = registry.list(
            TENANT_ID, PIPELINE_ID, 100_000L, Duration.ofSeconds(1))
            .await().indefinitely();

        assertEquals(1, staleRecords.size());
        assertEquals(PipelineWorkerState.STALE, staleRecords.get(0).state());
    }

    @Test
    void heartbeatResetsWorkerStateToHealthy() {
        resource.register(TENANT_ID, PIPELINE_ID, AUTH, request()).await().indefinitely();

        Response heartbeatResponse = resource.heartbeat(TENANT_ID, PIPELINE_ID, "worker-1", AUTH)
            .await().indefinitely();

        assertEquals(200, heartbeatResponse.getStatus());
        PipelineWorkerRecord record = assertInstanceOf(PipelineWorkerRecord.class, heartbeatResponse.getEntity());
        assertEquals(PipelineWorkerState.HEALTHY, record.state());
    }

    @Test
    void listReturnsEmptyForTenantWithNoWorkers() {
        Response response = resource.list("other-tenant", PIPELINE_ID, AUTH, null, null)
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(0, assertInstanceOf(List.class, response.getEntity()).size());
    }

    private static HostedWorkerRegisterRequest request() {
        return new HostedWorkerRegisterRequest(
            "worker-1",
            "sha256:contract",
            "sha256:release",
            "rest",
            "http://localhost:8181",
            "restaurant-artifact",
            "sha256:artifact");
    }
}
