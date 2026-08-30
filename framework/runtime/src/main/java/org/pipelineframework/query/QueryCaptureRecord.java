package org.pipelineframework.query;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.pipelineframework.connector.QueryObservation;
import org.pipelineframework.connector.QueryObservationOrigin;

/**
 * Captured output for a query step in one managed pipeline execution.
 */
public record QueryCaptureRecord(
    String tenantId,
    String executionId,
    int stepIndex,
    String queryId,
    String queryVersion,
    String captureKey,
    String inputJson,
    String outputJson,
    String outputType,
    Instant capturedAt,
    QueryCaptureStatus status,
    String outcomeCode,
    Optional<QueryObservation> observation
) {
    public QueryCaptureRecord {
        requireText(tenantId, "tenantId");
        requireText(executionId, "executionId");
        if (stepIndex < 0) {
            throw new IllegalArgumentException("stepIndex must be non-negative");
        }
        requireText(queryId, "queryId");
        requireText(queryVersion, "queryVersion");
        requireText(captureKey, "captureKey");
        status = Objects.requireNonNull(status, "status must not be null");
        outcomeCode = QueryFailureCode.require(outcomeCode);
        observation = Objects.requireNonNull(observation, "observation must not be null");
        if (observation.map(QueryObservation::origin)
            .filter(QueryObservationOrigin.CAPTURE_REPLAY::equals)
            .isPresent()) {
            throw new IllegalArgumentException("query captures must persist live provider observations");
        }
        if (outputJson == null) {
            throw new IllegalArgumentException("outputJson must not be null");
        }
        if (outputType == null) {
            throw new IllegalArgumentException("outputType must not be null");
        }
        if (status == QueryCaptureStatus.FOUND) {
            requireText(outputJson, "outputJson");
            requireText(outputType, "outputType");
        } else if (!outputJson.isEmpty() || !outputType.isEmpty()) {
            throw new IllegalArgumentException("not-found query captures must not contain output data");
        }
        capturedAt = capturedAt == null ? Instant.now() : capturedAt;
    }

    /**
     * Reads both the current outcome-aware shape and the legacy Found-only JSON shape.
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public static QueryCaptureRecord fromJson(
        @JsonProperty("tenantId") String tenantId,
        @JsonProperty("executionId") String executionId,
        @JsonProperty("stepIndex") int stepIndex,
        @JsonProperty("queryId") String queryId,
        @JsonProperty("queryVersion") String queryVersion,
        @JsonProperty("captureKey") String captureKey,
        @JsonProperty("inputJson") String inputJson,
        @JsonProperty("outputJson") String outputJson,
        @JsonProperty("outputType") String outputType,
        @JsonProperty("capturedAt") Instant capturedAt,
        @JsonProperty("status") QueryCaptureStatus status,
        @JsonProperty("outcomeCode") String outcomeCode,
        @JsonProperty("observation") QueryObservation observation
    ) {
        QueryCaptureStatus resolvedStatus = status == null ? QueryCaptureStatus.FOUND : status;
        String resolvedCode = outcomeCode == null && resolvedStatus == QueryCaptureStatus.FOUND
            ? "found"
            : outcomeCode;
        return new QueryCaptureRecord(
            tenantId,
            executionId,
            stepIndex,
            queryId,
            queryVersion,
            captureKey,
            inputJson,
            outputJson,
            outputType,
            capturedAt,
            resolvedStatus,
            resolvedCode,
            Optional.ofNullable(observation));
    }

    public QueryCaptureRecord(
        String tenantId,
        String executionId,
        int stepIndex,
        String queryId,
        String queryVersion,
        String captureKey,
        String inputJson,
        String outputJson,
        String outputType,
        Instant capturedAt
    ) {
        this(
            tenantId,
            executionId,
            stepIndex,
            queryId,
            queryVersion,
            captureKey,
            inputJson,
            outputJson,
            outputType,
            capturedAt,
            QueryCaptureStatus.FOUND,
            "found",
            Optional.empty());
    }

    public QueryCaptureRecord(
        String tenantId,
        String executionId,
        int stepIndex,
        String queryId,
        String queryVersion,
        String captureKey,
        String inputJson,
        String outputJson,
        String outputType,
        Instant capturedAt,
        QueryCaptureStatus status,
        String outcomeCode
    ) {
        this(
            tenantId,
            executionId,
            stepIndex,
            queryId,
            queryVersion,
            captureKey,
            inputJson,
            outputJson,
            outputType,
            capturedAt,
            status,
            outcomeCode,
            Optional.empty());
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
    }
}
