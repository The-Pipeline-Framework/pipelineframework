package org.pipelineframework.awaitable.admission;

import java.util.Objects;

/**
 * Provider-facing namespace for a durable await admission budget.
 *
 * <p>The tenant is intentionally excluded: one provider budget protects the
 * provider across all tenants and runtime replicas.</p>
 */
public record AwaitAdmissionScope(String pipelineId, String stepId, String endpoint) {
    public AwaitAdmissionScope {
        pipelineId = requireText(pipelineId, "pipelineId");
        stepId = requireText(stepId, "stepId");
        endpoint = requireText(endpoint, "endpoint");
    }

    public String key() {
        return pipelineId.length() + ":" + pipelineId
            + stepId.length() + ":" + stepId
            + endpoint.length() + ":" + endpoint;
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
