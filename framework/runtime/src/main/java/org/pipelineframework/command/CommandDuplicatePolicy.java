package org.pipelineframework.command;

/**
 * Duplicate handling policy for managed command steps.
 */
public enum CommandDuplicatePolicy {
    RETURN_RECORDED,
    FAIL;

    public static CommandDuplicatePolicy fromString(String value) {
        if (value == null || value.isBlank()) {
            return RETURN_RECORDED;
        }
        for (CommandDuplicatePolicy policy : values()) {
            if (policy.name().equalsIgnoreCase(value.trim())) {
                return policy;
            }
        }
        throw new IllegalArgumentException("Unsupported command duplicate policy: " + value);
    }
}
