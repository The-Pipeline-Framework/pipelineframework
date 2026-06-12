package org.pipelineframework.orchestrator.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
