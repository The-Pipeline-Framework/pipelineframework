package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Local in-process tenant admission and per-tenant transition quota policy.
 */
@ApplicationScoped
public class LocalControlPlaneAdmissionPolicy implements ControlPlaneAdmissionPolicy {

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private final ConcurrentHashMap<String, AtomicInteger> activeTransitionsByTenant = new ConcurrentHashMap<>();

    public LocalControlPlaneAdmissionPolicy() {
    }

    public LocalControlPlaneAdmissionPolicy(PipelineOrchestratorConfig orchestratorConfig) {
        this.orchestratorConfig = orchestratorConfig;
    }

    @Override
    public ControlPlaneAdmissionDecision admit(ControlPlaneAdmissionRequest request) {
        if (request == null) {
            return ControlPlaneAdmissionDecision.deny("INVALID_ADMISSION_REQUEST", "Admission request is required");
        }
        if (requireExplicitTenant()
            && request.operation() == ControlPlaneAdmissionOperation.SUBMIT_EXECUTION
            && !request.explicitTenant()) {
            return ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_REQUIRED,
                "Tenant id is required for async execution submission");
        }
        String tenantId = effectiveTenantId(request);
        List<String> allowedTenants = allowedTenants();
        if (!allowedTenants.isEmpty() && !allowedTenants.contains(tenantId)) {
            return ControlPlaneAdmissionDecision.deny(
                ControlPlaneAdmissionDecision.TENANT_NOT_ALLOWED,
                "Tenant '" + request.tenantId() + "' is not allowed for local control-plane operations");
        }
        return ControlPlaneAdmissionDecision.allow();
    }

    @Override
    public ControlPlaneTransitionAdmission admitTransition(ControlPlaneAdmissionRequest request) {
        ControlPlaneAdmissionDecision decision = admit(request);
        if (!decision.allowed()) {
            return ControlPlaneTransitionAdmission.denied(decision);
        }
        int maxInFlight = maxInFlightTransitionsPerTenant();
        if (maxInFlight <= 0) {
            return ControlPlaneTransitionAdmission.admitted(ControlPlaneTransitionPermit.noop());
        }
        String tenantId = effectiveTenantId(request);
        AtomicInteger counter = activeTransitionsByTenant.computeIfAbsent(tenantId, ignored -> new AtomicInteger());
        while (true) {
            int current = counter.get();
            if (current >= maxInFlight) {
                return ControlPlaneTransitionAdmission.denied(ControlPlaneAdmissionDecision.deny(
                    ControlPlaneAdmissionDecision.TENANT_TRANSITION_QUOTA_SATURATED,
                    "Tenant '" + tenantId + "' has reached max in-flight transitions "
                        + maxInFlight));
            }
            if (counter.compareAndSet(current, current + 1)) {
                return ControlPlaneTransitionAdmission.admitted(() -> releasePermit(tenantId, counter));
            }
        }
    }

    private void releasePermit(String tenantId, AtomicInteger counter) {
        activeTransitionsByTenant.computeIfPresent(tenantId, (ignored, activeCounter) -> {
            int remaining = activeCounter.updateAndGet(value -> Math.max(0, value - 1));
            return remaining == 0 ? null : activeCounter;
        });
    }

    private String effectiveTenantId(ControlPlaneAdmissionRequest request) {
        String tenantId = request == null ? null : request.tenantId();
        return tenantId == null ? "" : tenantId.trim();
    }

    private boolean requireExplicitTenant() {
        PipelineOrchestratorConfig.TenancyConfig tenancy = orchestratorConfig == null ? null : orchestratorConfig.tenancy();
        return tenancy != null && tenancy.requireExplicitTenant();
    }

    private List<String> allowedTenants() {
        PipelineOrchestratorConfig.TenancyConfig tenancy = orchestratorConfig == null ? null : orchestratorConfig.tenancy();
        if (tenancy == null || tenancy.allowedTenants() == null) {
            return List.of();
        }
        return tenancy.allowedTenants().stream()
            .filter(value -> value != null && !value.isBlank())
            .map(String::trim)
            .toList();
    }

    private int maxInFlightTransitionsPerTenant() {
        PipelineOrchestratorConfig.QuotaConfig quotas = orchestratorConfig == null ? null : orchestratorConfig.quotas();
        return quotas == null ? 0 : Math.max(0, quotas.maxInFlightTransitionsPerTenant());
    }
}
