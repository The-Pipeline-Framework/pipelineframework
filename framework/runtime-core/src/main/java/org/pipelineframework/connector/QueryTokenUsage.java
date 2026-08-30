package org.pipelineframework.connector;

import java.util.Objects;
import java.util.OptionalLong;

/** Provider-reported token counts. Each count is independently optional and is never derived. */
public record QueryTokenUsage(
    OptionalLong inputTokens,
    OptionalLong outputTokens,
    OptionalLong totalTokens
) {
    public QueryTokenUsage {
        inputTokens = requireNonNegative(inputTokens, "inputTokens");
        outputTokens = requireNonNegative(outputTokens, "outputTokens");
        totalTokens = requireNonNegative(totalTokens, "totalTokens");
    }

    public static QueryTokenUsage empty() {
        return new QueryTokenUsage(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty());
    }

    private static OptionalLong requireNonNegative(OptionalLong value, String field) {
        Objects.requireNonNull(value, field + " must not be null");
        if (value.isPresent() && value.getAsLong() < 0) {
            throw new IllegalArgumentException(field + " must be non-negative when present");
        }
        return value;
    }
}
