package org.pipelineframework.orchestrator;

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ControlPlaneAdmissionPolicyTest {

    @Test
    void defaultPolicyAllowsAllTenantsAndDoesNotRequireExplicitTenant() {
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy();

        ControlPlaneAdmissionDecision decision = policy.admit(request("tenant-a", false));

        assertTrue(decision.allowed());
    }

    @Test
    void allowedTenantListRejectsUnknownTenant() {
        PipelineOrchestratorConfig config = config(List.of("tenant-a", "tenant-b"), false, 0);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);

        ControlPlaneAdmissionDecision allowed = policy.admit(request("tenant-a", true));
        ControlPlaneAdmissionDecision denied = policy.admit(request("tenant-c", true));

        assertTrue(allowed.allowed());
        assertFalse(denied.allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_NOT_ALLOWED, denied.errorCode());
    }

    @Test
    void explicitTenantRequirementAppliesToSubmission() {
        PipelineOrchestratorConfig config = config(List.of(), true, 0);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);

        ControlPlaneAdmissionDecision decision = policy.admit(request("default", false));

        assertFalse(decision.allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_REQUIRED, decision.errorCode());
    }

    @Test
    void perTenantTransitionQuotaSaturatesAndReleases() {
        PipelineOrchestratorConfig config = config(List.of(), false, 1);
        LocalControlPlaneAdmissionPolicy policy = new LocalControlPlaneAdmissionPolicy(config);
        ControlPlaneAdmissionRequest request = transitionRequest("tenant-a");

        ControlPlaneTransitionAdmission first = policy.admitTransition(request);
        ControlPlaneTransitionAdmission saturated = policy.admitTransition(request);
        first.permit().close();
        ControlPlaneTransitionAdmission afterRelease = policy.admitTransition(request);
        afterRelease.permit().close();

        assertTrue(first.decision().allowed());
        assertFalse(saturated.decision().allowed());
        assertEquals(ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED, saturated.decision().errorCode());
        assertTrue(afterRelease.decision().allowed());
    }

    @Test
    void admissionDecisionRejectsAllowedErrorDetails() {
        assertThrows(IllegalArgumentException.class,
            () -> new ControlPlaneAdmissionDecision(true, "ERROR", "reason"));
    }

    @Test
    void deniedTransitionAdmissionRequiresDeniedDecision() {
        assertThrows(IllegalArgumentException.class,
            () -> ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.allow()));
    }

    @Test
    void admissionExceptionRequiresDeniedDecision() {
        assertThrows(IllegalArgumentException.class,
            () -> new ControlPlaneAdmissionException(ControlPlaneAdmissionDecision.allow()));
    }

    private static ControlPlaneAdmissionRequest request(String tenantId, boolean explicitTenant) {
        return new ControlPlaneAdmissionRequest(
            tenantId,
            ControlPlaneAdmissionOperation.SUBMIT_EXECUTION,
            "pipeline-1",
            "bundle-1",
            null,
            "api",
            explicitTenant);
    }

    private static ControlPlaneAdmissionRequest transitionRequest(String tenantId) {
        return new ControlPlaneAdmissionRequest(
            tenantId,
            ControlPlaneAdmissionOperation.PROCESS_WORK_ITEM,
            "pipeline-1",
            "bundle-1",
            "exec-1",
            "worker-dispatch",
            true);
    }

    private static PipelineOrchestratorConfig config(
        List<String> allowedTenants,
        boolean requireExplicitTenant,
        int maxInFlightTransitionsPerTenant) {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.TenancyConfig tenancy = mock(PipelineOrchestratorConfig.TenancyConfig.class);
        PipelineOrchestratorConfig.QuotaConfig quotas = mock(PipelineOrchestratorConfig.QuotaConfig.class);
        when(config.tenancy()).thenReturn(tenancy);
        when(config.quotas()).thenReturn(quotas);
        when(tenancy.allowedTenants()).thenReturn(allowedTenants);
        when(tenancy.requireExplicitTenant()).thenReturn(requireExplicitTenant);
        when(quotas.maxInFlightTransitionsPerTenant()).thenReturn(maxInFlightTransitionsPerTenant);
        return config;
    }
}
