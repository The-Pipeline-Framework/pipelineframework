package org.pipelineframework.connector;

import java.util.Objects;
import java.util.Optional;

/** Provider-neutral metadata about one completed Query observation. */
public record QueryObservation(
    Optional<QueryTokenUsage> tokenUsage,
    Optional<String> responseModel,
    Optional<String> finishReason,
    QueryObservationOrigin origin
) {
    private static final int MAX_RESPONSE_MODEL_LENGTH = 256;
    private static final int MAX_FINISH_REASON_LENGTH = 128;

    public QueryObservation {
        tokenUsage = Objects.requireNonNull(tokenUsage, "tokenUsage must not be null");
        responseModel = requireBoundedText(responseModel, "responseModel", MAX_RESPONSE_MODEL_LENGTH);
        finishReason = requireBoundedText(finishReason, "finishReason", MAX_FINISH_REASON_LENGTH);
        origin = Objects.requireNonNull(origin, "origin must not be null");
    }

    public static QueryObservation live(
        Optional<QueryTokenUsage> tokenUsage,
        Optional<String> responseModel,
        Optional<String> finishReason
    ) {
        return new QueryObservation(tokenUsage, responseModel, finishReason, QueryObservationOrigin.LIVE_PROVIDER);
    }

    public QueryObservation asReplay() {
        return origin == QueryObservationOrigin.CAPTURE_REPLAY
            ? this
            : new QueryObservation(tokenUsage, responseModel, finishReason, QueryObservationOrigin.CAPTURE_REPLAY);
    }

    private static Optional<String> requireBoundedText(Optional<String> value, String field, int maximumLength) {
        Objects.requireNonNull(value, field + " must not be null");
        value.ifPresent(text -> {
            if (text.isBlank() || text.length() > maximumLength) {
                throw new IllegalArgumentException(
                    field + " must be non-blank and at most " + maximumLength + " characters when present");
            }
        });
        return value;
    }
}
