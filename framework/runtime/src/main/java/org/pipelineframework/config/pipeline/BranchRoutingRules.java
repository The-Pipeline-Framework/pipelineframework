package org.pipelineframework.config.pipeline;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

    public static <X extends RuntimeException> void rejectPredicateKeys(
        Map<?, ?> stepMap,
        String stepName,
        Function<String, X> exceptionFactory
    ) {
        List<String> rejected = rejectedPredicateKeys(stepMap);
        if (rejected.isEmpty()) {
            return;
        }
        throw exceptionFactory.apply(unsupportedPredicateRoutingMessage(stepName, rejected));
    }

    static String unsupportedPredicateRoutingMessage(String stepName, List<String> rejected) {
        return "Step '" + (stepName == null ? "<unnamed>" : stepName)
            + "' declares unsupported predicate-style routing keys: " + String.join(", ", rejected)
            + ". Use type-based accepts/terminal routing only.";
    }
}
