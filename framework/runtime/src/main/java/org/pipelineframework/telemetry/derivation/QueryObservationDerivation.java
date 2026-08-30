package org.pipelineframework.telemetry.derivation;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryObservationOrigin;
import org.pipelineframework.connector.QueryTokenUsage;

/** Pure derivation of bounded telemetry values from framework-owned Query observations. */
public final class QueryObservationDerivation {
    private QueryObservationDerivation() {
    }

    public static Signal derive(ConnectorOperationIdentity operation, QueryObservation observation) {
        Objects.requireNonNull(operation, "query operation identity must not be null");
        Objects.requireNonNull(observation, "query observation must not be null");
        QueryTokenUsage usage = observation.tokenUsage().orElseGet(QueryTokenUsage::empty);
        return new Signal(
            operation.providerId().value(),
            operation.operationId(),
            observation.origin() == QueryObservationOrigin.CAPTURE_REPLAY,
            usage.inputTokens(),
            usage.outputTokens(),
            usage.totalTokens(),
            observation.responseModel(),
            observation.finishReason());
    }

    public record Signal(
        String provider,
        String operation,
        boolean replayed,
        OptionalLong inputTokens,
        OptionalLong outputTokens,
        OptionalLong totalTokens,
        Optional<String> responseModel,
        Optional<String> finishReason
    ) {
        public Signal {
            provider = requireText(provider, "provider");
            operation = requireText(operation, "operation");
            inputTokens = Objects.requireNonNull(inputTokens, "inputTokens must not be null");
            outputTokens = Objects.requireNonNull(outputTokens, "outputTokens must not be null");
            totalTokens = Objects.requireNonNull(totalTokens, "totalTokens must not be null");
            responseModel = Objects.requireNonNull(responseModel, "responseModel must not be null");
            finishReason = Objects.requireNonNull(finishReason, "finishReason must not be null");
        }

        private static String requireText(String value, String field) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(field + " must not be blank");
            }
            return value;
        }
    }
}
