package org.pipelineframework.blocks.openapi.mapping;

import java.util.List;
import java.util.Objects;

/** Bounded authoring state for one imported operation representation boundary. */
public record OpenApiMappingState(
    String operationIdentity,
    String direction,
    String sourceSchemaJson,
    String targetSchemaJson,
    String sourceSchemaFingerprint,
    String targetSchemaFingerprint,
    List<String> diagnostics,
    int turn,
    int maxTurns
) {
    public static final int MAX_TURNS = 8;

    public OpenApiMappingState {
        operationIdentity = OpenApiMappingJson.require(operationIdentity, "OpenAPI operation identity");
        direction = OpenApiMappingJson.require(direction, "OpenAPI mapping direction").toUpperCase(java.util.Locale.ROOT);
        if (!direction.equals("REQUEST") && !direction.equals("RESPONSE")) {
            throw new IllegalArgumentException("OpenAPI mapping direction must be REQUEST or RESPONSE");
        }
        sourceSchemaJson = OpenApiMappingJson.require(sourceSchemaJson, "OpenAPI source schema");
        targetSchemaJson = OpenApiMappingJson.require(targetSchemaJson, "OpenAPI target schema");
        sourceSchemaFingerprint = fingerprint(sourceSchemaJson, sourceSchemaFingerprint, "source");
        targetSchemaFingerprint = fingerprint(targetSchemaJson, targetSchemaFingerprint, "target");
        diagnostics = List.copyOf(Objects.requireNonNull(diagnostics, "OpenAPI mapping diagnostics must not be null"));
        if (diagnostics.size() > 32) throw new IllegalArgumentException("OpenAPI mapping diagnostics exceed limit");
        if (turn < 0 || maxTurns < 1 || maxTurns > MAX_TURNS || turn > maxTurns) {
            throw new IllegalArgumentException("OpenAPI mapping turn bounds are invalid");
        }
    }

    public static OpenApiMappingState start(
        String operationIdentity,
        String direction,
        String sourceSchemaJson,
        String targetSchemaJson,
        int maxTurns
    ) {
        return new OpenApiMappingState(operationIdentity, direction, sourceSchemaJson, targetSchemaJson,
            OpenApiMappingJson.fingerprint(sourceSchemaJson, "OpenAPI source schema"),
            OpenApiMappingJson.fingerprint(targetSchemaJson, "OpenAPI target schema"), List.of(), 0, maxTurns);
    }

    OpenApiMappingState retry(List<String> nextDiagnostics) {
        return new OpenApiMappingState(operationIdentity, direction, sourceSchemaJson, targetSchemaJson,
            sourceSchemaFingerprint, targetSchemaFingerprint, nextDiagnostics, turn + 1, maxTurns);
    }

    private static String fingerprint(String schema, String supplied, String direction) {
        String checked = OpenApiMappingJson.require(supplied, "OpenAPI " + direction + " schema fingerprint")
            .toLowerCase(java.util.Locale.ROOT);
        String actual = OpenApiMappingJson.fingerprint(schema, "OpenAPI " + direction + " schema");
        if (!checked.equals(actual)) {
            throw new IllegalArgumentException("OpenAPI " + direction + " schema fingerprint does not match schema");
        }
        return checked;
    }
}
