package org.pipelineframework.orchestrator;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Caches immutable durable codec plans by pinned release and root canonical type expression. */
public final class DurablePayloadPlanRegistry {
    private final ConcurrentHashMap<DurablePayloadReleaseCoordinate, Map<String, CompiledDurablePayloadPlan>> plans = new ConcurrentHashMap<>();

    public void activate(DurablePayloadReleaseCoordinate release, Map<String, CanonicalPayloadBinding> bindings) {
        Map<String, CompiledDurablePayloadPlan> compiled = bindings.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
            Map.Entry::getKey, entry -> CompiledDurablePayloadPlan.compile(entry.getValue())));
        plans.compute(release, (ignored, existing) -> {
            Map<String, CompiledDurablePayloadPlan> merged = new java.util.HashMap<>(
                existing == null ? Map.of() : existing);
            compiled.forEach((expression, plan) -> {
                CompiledDurablePayloadPlan prior = merged.putIfAbsent(expression, plan);
                if (prior != null && !prior.binding().equals(plan.binding())) {
                    throw new IllegalStateException("Conflicting durable payload binding for release " + release
                        + " and type expression " + expression);
                }
            });
            return Map.copyOf(merged);
        });
    }

    public CompiledDurablePayloadPlan plan(DurablePayloadReleaseCoordinate release, String typeExpressionFingerprint) {
        CompiledDurablePayloadPlan plan = plans.getOrDefault(release, Map.of()).get(typeExpressionFingerprint);
        if (plan == null) {
            throw new IllegalStateException("No durable payload plan for release " + release
                + " and type expression " + typeExpressionFingerprint);
        }
        return plan;
    }
}
