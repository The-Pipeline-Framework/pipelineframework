package org.pipelineframework.config.pipeline;

import java.util.List;
import java.util.Map;

/**
 * Shared authored-YAML guardrails for branch-aware routing.
 */
public final class BranchRoutingRules {

    public static final List<String> REJECTED_BRANCH_PREDICATE_KEYS =
        List.of("when", "condition", "predicate", "expression");

    private BranchRoutingRules() {
    }

    public static List<String> rejectedPredicateKeys(Map<?, ?> stepMap) {
        if (stepMap == null || stepMap.isEmpty()) {
            return List.of();
        }
        return REJECTED_BRANCH_PREDICATE_KEYS.stream()
            .filter(stepMap::containsKey)
            .sorted()
            .toList();
    }
}
