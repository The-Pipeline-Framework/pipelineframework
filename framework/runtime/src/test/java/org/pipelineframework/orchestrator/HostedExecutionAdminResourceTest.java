package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.orchestrator.dto.HostedExecutionRedriveRequest;

class HostedExecutionAdminResourceTest {

    private static final String TENANT_ID = "tenant-1";
    private static final String EXECUTION_ID = "exec-1";
    private static final String TOKEN = "admin-token";
    private static final String AUTH = "Bearer " + TOKEN;

    private HostedExecutionAdminResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.AdminConfig adminConfig;
    private PipelineControlPlane controlPlane;

    @BeforeEach
    void setUp() {
        resource = new HostedExecutionAdminResource();
        config = mock(PipelineOrchestratorConfig.class);
        adminConfig = mock(PipelineOrchestratorConfig.AdminConfig.class);
        controlPlane = mock(PipelineControlPlane.class);
        resource.orchestratorConfig = config;
        resource.controlPlane = controlPlane;
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        when(config.admin()).thenReturn(adminConfig);
        when(adminConfig.enabled()).thenReturn(true);
        when(adminConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());
    }

    @Test
    void validateConfigRejectsMissingAdminTokenWhenEnabled() {
        when(adminConfig.adminToken()).thenReturn(Optional.empty());

        IllegalStateException error = assertThrows(IllegalStateException.class, resource::validateConfig);

        assertEquals(
            "pipeline.orchestrator.admin.admin-token or pipeline.orchestrator.admin.admin-token-ref "
                + "is required when pipeline.orchestrator.admin.enabled=true",
            error.getMessage());
    }

    @Test
    void disabledResourceReturnsNotFound() {
        when(adminConfig.enabled()).thenReturn(false);

        Response response = resource.redrive(TENANT_ID, EXECUTION_ID, AUTH, null).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void rejectsMissingAndBadToken() {
        assertEquals(401, resource.redrive(TENANT_ID, EXECUTION_ID, null, null)
            .await().indefinitely().getStatus());
        assertEquals(401, resource.redrive(TENANT_ID, EXECUTION_ID, "Bearer wrong", null)
            .await().indefinitely().getStatus());
    }

    @Test
    void missingExecutionReturnsNotFound() {
        when(controlPlane.redriveExecution(TENANT_ID, EXECUTION_ID, null, false, null))
            .thenReturn(Uni.createFrom().failure(new NotFoundException("Execution not found")));

        Response response = resource.redrive(TENANT_ID, EXECUTION_ID, AUTH, null).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void invalidExecutionStateReturnsConflict() {
        when(controlPlane.redriveExecution(TENANT_ID, EXECUTION_ID, 4L, false, "retry"))
            .thenReturn(Uni.createFrom().failure(new IllegalStateException("Execution is not terminal")));

        Response response = resource.redrive(
                TENANT_ID,
                EXECUTION_ID,
                AUTH,
                new HostedExecutionRedriveRequest(4L, "retry", false))
            .await().indefinitely();

        assertEquals(409, response.getStatus());
    }

    @Test
    void redriveDelegatesAndReturnsSummary() {
        ExecutionRedriveResult result = new ExecutionRedriveResult(
            TENANT_ID,
            EXECUTION_ID,
            ExecutionStatus.DLQ,
            ExecutionStatus.QUEUED,
            8L,
            2,
            3,
            "pipeline-a",
            "contract-a",
            "release-a",
            100L);
        when(controlPlane.redriveExecution(TENANT_ID, EXECUTION_ID, 7L, true, "operator retry"))
            .thenReturn(Uni.createFrom().item(result));

        Response response = resource.redrive(
                TENANT_ID,
                EXECUTION_ID,
                AUTH,
                new HostedExecutionRedriveRequest(7L, "operator retry", true))
            .await().indefinitely();

        assertEquals(200, response.getStatus());
        assertEquals(result, assertInstanceOf(ExecutionRedriveResult.class, response.getEntity()));
        verify(controlPlane).redriveExecution(TENANT_ID, EXECUTION_ID, 7L, true, "operator retry");
    }
}
