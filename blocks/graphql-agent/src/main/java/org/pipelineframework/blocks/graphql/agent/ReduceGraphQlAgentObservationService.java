package org.pipelineframework.blocks.graphql.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.graphql.GraphQlDataJson;
import org.pipelineframework.connector.graphql.GraphQlError;
import org.pipelineframework.connector.graphql.GraphQlResult;
import org.pipelineframework.connector.graphql.GraphQlVariablesJson;
import org.pipelineframework.service.ReactiveService;

/** Rebuilds trusted state from inert context and normalized GraphQL observations. */
@ApplicationScoped
public final class ReduceGraphQlAgentObservationService
    implements ReactiveService<OperationObservation, GraphQlAgentProgress> {
    private static final ObjectMapper JSON = PipelineJson.mapper();

    @Override
    public Uni<GraphQlAgentProgress> process(OperationObservation observation) {
        try {
            return Uni.createFrom().item(reduce(observation));
        } catch (Exception failure) {
            return Uni.createFrom().failure(failure);
        }
    }

    static GraphQlAgentProgress reduce(OperationObservation observation) {
        Objects.requireNonNull(observation, "GraphQL operation observation must not be null");
        ObservationValues values = observation instanceof OperationObservation.Result result
            ? result(result.value())
            : empty(((OperationObservation.Empty) observation).value());
        GraphQlAgentState next = values.state().append(new GraphQlAgentObservation(
            values.state().turn(), values.operationKey(), values.kind(), values.variablesJson(), values.outcome(),
            values.code(), values.result().data().map(GraphQlDataJson::value).orElse("{}"), values.result().errors()));
        if (next.turn() >= next.maxTurns()) {
            return new GraphQlAgentProgress.Complete(new GraphQlAgentCompletion(
                "GraphQL agent stopped after reaching its configured turn bound."));
        }
        return new GraphQlAgentProgress.Recur(next);
    }

    private static ObservationValues result(OperationResultObservation observation) {
        GraphQlResult result = result(observation.resultJson());
        return values(observation.kind(), observation.outcome(), observation.code(), observation.argumentsJson(),
            observation.contextJson(), result);
    }

    private static ObservationValues empty(OperationEmptyObservation observation) {
        return values(observation.kind(), observation.outcome(), observation.code(), observation.argumentsJson(),
            observation.contextJson(), new EmptyGraphQlResult().value());
    }

    private static ObservationValues values(
        String dispatchKind,
        String outcome,
        String code,
        String argumentsJson,
        String contextJson,
        GraphQlResult result
    ) {
        try {
            GraphQlAgentState state = JSON.readValue(contextJson, TrustedContext.class).state();
            JsonNode arguments = JSON.readTree(argumentsJson);
            String operationKey = requiredText(arguments, "operationKey");
            GraphQlVariablesJson variables = new GraphQlVariablesJson(requiredText(arguments, "variablesJson"));
            String kind = dispatchKind.toLowerCase(java.util.Locale.ROOT).contains("command") ? "MUTATION" : "QUERY";
            return new ObservationValues(state, operationKey, kind, variables, outcome,
                Objects.requireNonNull(code, "GraphQL observation code must not be null"), result);
        } catch (Exception failure) {
            throw new IllegalArgumentException("GraphQL operation observation is not canonical", failure);
        }
    }

    private static GraphQlResult result(String resultJson) {
        try {
            JsonNode root = JSON.readTree(resultJson);
            Optional<GraphQlDataJson> data = data(root.path("data"));
            var errors = new ArrayList<GraphQlError>();
            for (JsonNode error : root.path("errors")) {
                var path = new ArrayList<String>();
                error.path("path").forEach(segment -> path.add(segment.asText()));
                errors.add(new GraphQlError(requiredText(error, "code"), path, requiredText(error, "message")));
            }
            return new GraphQlResult(data, errors);
        } catch (Exception failure) {
            throw new IllegalArgumentException("GraphQL operation result is not canonical", failure);
        }
    }

    private static Optional<GraphQlDataJson> data(JsonNode data) {
        if (data.isMissingNode() || data.isNull()) {
            return Optional.empty();
        }
        if (!data.isTextual()) {
            throw new IllegalArgumentException("GraphQL operation result data must be canonical textual JSON");
        }
        return Optional.of(new GraphQlDataJson(data.asText()));
    }

    private static String requiredText(JsonNode object, String field) {
        JsonNode value = object.path(field);
        if (!value.isTextual() || value.asText().isBlank()) {
            throw new IllegalArgumentException("GraphQL observation " + field + " must be a non-blank string");
        }
        return value.asText();
    }

    private record TrustedContext(GraphQlAgentState state) {
        private TrustedContext {
            state = Objects.requireNonNull(state, "GraphQL trusted state must not be null");
        }
    }

    private record ObservationValues(
        GraphQlAgentState state,
        String operationKey,
        String kind,
        GraphQlVariablesJson variablesJson,
        String outcome,
        String code,
        GraphQlResult result
    ) {
    }

    /** Empty Query observations still become a meaningful state observation without inventing provider data. */
    private record EmptyGraphQlResult() {
        GraphQlResult value() {
            return new GraphQlResult(Optional.empty(), List.of(new GraphQlError(
                "no-data", List.of(), "The operation returned no GraphQL result.")));
        }
    }
}
