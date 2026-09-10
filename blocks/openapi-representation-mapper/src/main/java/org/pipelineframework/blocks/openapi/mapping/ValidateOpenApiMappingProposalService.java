package org.pipelineframework.blocks.openapi.mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.http.HttpRepresentationMappingOptions;
import org.pipelineframework.service.ReactiveService;

/** Validates the bounded provider option language and every proposed schema path without executing model output. */
@ApplicationScoped
public final class ValidateOpenApiMappingProposalService
    implements ReactiveService<OpenApiMappingCandidate, OpenApiMappingProgress> {
    private static final TypeReference<Map<String, Object>> OPTIONS = new TypeReference<>() { };

    @Override
    public Uni<OpenApiMappingProgress> process(OpenApiMappingCandidate candidate) {
        return Uni.createFrom().item(() -> validate(candidate));
    }

    static OpenApiMappingProgress validate(OpenApiMappingCandidate candidate) {
        OpenApiMappingState state = candidate.state();
        List<String> diagnostics = new ArrayList<>();
        String canonicalOptions = candidate.mappingOptionsJson();
        try {
            JsonNode optionJson = PipelineJson.mapper().readTree(candidate.mappingOptionsJson());
            if (optionJson == null || !optionJson.isObject()) {
                throw new IllegalArgumentException("mapping options must be a JSON object");
            }
            HttpRepresentationMappingOptions options = HttpRepresentationMappingOptions.from(
                PipelineJson.mapper().convertValue(optionJson, OPTIONS));
            JsonNode source = OpenApiMappingJson.schema(state.sourceSchemaJson(), "OpenAPI source schema");
            JsonNode target = OpenApiMappingJson.schema(state.targetSchemaJson(), "OpenAPI target schema");
            validatePaths(options, source, target);
            canonicalOptions = PipelineJson.mapper().writeValueAsString(optionJson);
        } catch (Exception invalid) {
            diagnostics.add(message(invalid));
        }
        if (diagnostics.isEmpty()) {
            return new OpenApiMappingProgress.Complete(new OpenApiRepresentationMappingProposal(
                state.operationIdentity(), state.direction(), state.sourceSchemaFingerprint(),
                state.targetSchemaFingerprint(), canonicalOptions, true, List.of(), state.turn() + 1));
        }
        OpenApiMappingState retry = state.retry(diagnostics);
        return retry.turn() >= retry.maxTurns()
            ? new OpenApiMappingProgress.Complete(OpenApiRepresentationMappingProposal.exhausted(retry))
            : new OpenApiMappingProgress.Recur(retry);
    }

    private static void validatePaths(
        HttpRepresentationMappingOptions options,
        JsonNode source,
        JsonNode target
    ) {
        options.fields().forEach((canonical, wire) -> {
            requirePath(source, canonical, "canonical field");
            requirePath(target, wire, "wire field");
        });
        options.constants().keySet().forEach(path -> requirePath(target, path, "constant target"));
        options.enums().keySet().forEach(path -> {
            requirePath(source, path, "enum canonical field");
            requirePath(target, options.fields().getOrDefault(path, path), "enum wire field");
        });
        options.jsonObjects().forEach((canonical, wire) -> {
            requirePath(source, canonical, "JSON-object canonical field");
            requirePath(target, wire, "JSON-object wire field");
        });
        options.collections().forEach(collection -> {
            JsonNode canonicalItems = collectionItems(requirePath(source, collection.canonicalPath(),
                "collection canonical field"), "canonical");
            JsonNode wireItems = collectionItems(requirePath(target, collection.wirePath(),
                "collection wire field"), "wire");
            collection.fields().forEach((canonical, wire) -> {
                requirePath(canonicalItems, canonical, "collection canonical item field");
                requirePath(wireItems, wire, "collection wire item field");
            });
        });
        options.discriminator().ifPresent(discriminator -> {
            requirePath(source, discriminator.canonicalPath(), "discriminator canonical field");
            requirePath(target, discriminator.wirePath(), "discriminator wire field");
        });
    }

    private static JsonNode collectionItems(JsonNode schema, String side) {
        if (!"array".equals(schema.path("type").asText()) || !schema.path("items").isObject()) {
            throw new IllegalArgumentException("HTTP " + side + " collection path must select an array schema");
        }
        return schema.path("items");
    }

    private static JsonNode requirePath(JsonNode schema, String path, String label) {
        JsonNode current = schema;
        for (String segment : path.split("\\.")) {
            JsonNode properties = current.path("properties");
            if (!properties.isObject() || !properties.has(segment)) {
                throw new IllegalArgumentException("HTTP " + label + " path does not exist: " + path);
            }
            current = properties.path(segment);
        }
        return current;
    }

    private static String message(Exception failure) {
        String message = failure.getMessage();
        if (message == null || message.isBlank()) return failure.getClass().getSimpleName();
        return message.length() <= 512 ? message : message.substring(0, 512);
    }
}
