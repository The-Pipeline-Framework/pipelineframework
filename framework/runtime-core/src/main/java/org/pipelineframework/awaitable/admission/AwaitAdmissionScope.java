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
        return lengthPrefixedKey(pipelineId, stepId, endpoint);
    }

    /**
     * Encodes components without delimiter ambiguity.
     */
    public static String lengthPrefixedKey(String... components) {
        Objects.requireNonNull(components, "components must not be null");
        StringBuilder key = new StringBuilder();
        for (String component : components) {
            Objects.requireNonNull(component, "key component must not be null");
            key.append(component.length()).append(':').append(component);
        }
        return key.toString();
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
