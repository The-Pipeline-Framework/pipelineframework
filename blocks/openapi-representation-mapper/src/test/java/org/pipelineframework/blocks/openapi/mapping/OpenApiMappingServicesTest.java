package org.pipelineframework.blocks.openapi.mapping;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

class OpenApiMappingServicesTest {
    private static final String CANONICAL = """
        {"type":"object","properties":{"evidence":{"type":"string"}},"required":["evidence"]}
        """;
    private static final String WIRE = """
        {"type":"object","properties":{"payload":{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}},"required":["payload"]}
        """;

    @Test
    void acceptsABoundedProposalWhosePathsExistOnBothSchemas() {
        OpenApiMappingState state = OpenApiMappingState.start(
            "evidence.lookup:response", "response", CANONICAL, WIRE, 3);

        OpenApiMappingProgress.Complete complete = assertInstanceOf(OpenApiMappingProgress.Complete.class,
            ValidateOpenApiMappingProposalService.validate(new OpenApiMappingCandidate(
                state, "{\"fields\":{\"evidence\":\"payload.value\"}}")));

        assertTrue(complete.value().valid());
        assertEquals(state.sourceSchemaFingerprint(), complete.value().sourceSchemaFingerprint());
        assertEquals(state.targetSchemaFingerprint(), complete.value().targetSchemaFingerprint());
        assertEquals(1, complete.value().turns());
    }

    @Test
    void carriesDeterministicDiagnosticsIntoBoundedRecursion() {
        OpenApiMappingState state = OpenApiMappingState.start(
            "evidence.lookup:response", "RESPONSE", CANONICAL, WIRE, 2);

        OpenApiMappingProgress.Recur recur = assertInstanceOf(OpenApiMappingProgress.Recur.class,
            ValidateOpenApiMappingProposalService.validate(new OpenApiMappingCandidate(
                state, "{\"fields\":{\"missing\":\"payload.value\"}}")));
        assertEquals(1, recur.value().turn());
        assertTrue(recur.value().diagnostics().getFirst().contains("does not exist"));

        OpenApiMappingProgress.Complete exhausted = assertInstanceOf(OpenApiMappingProgress.Complete.class,
            ValidateOpenApiMappingProposalService.validate(new OpenApiMappingCandidate(
                recur.value(), "{\"fields\":{\"evidence\":\"payload.missing\"}}")));
        assertFalse(exhausted.value().valid());
        assertEquals(2, exhausted.value().turns());
    }

    @Test
    void rejectsTamperedSchemaFingerprintsAndStopsBeforeAnotherQueryAtTheBound() {
        OpenApiMappingState state = OpenApiMappingState.start(
            "evidence.lookup:response", "RESPONSE", CANONICAL, WIRE, 1);
        assertThrows(IllegalArgumentException.class, () -> new OpenApiMappingState(
            state.operationIdentity(), state.direction(), CANONICAL, WIRE, "0".repeat(64),
            state.targetSchemaFingerprint(), List.of(), 0, 1));

        OpenApiMappingTurnGate.Complete complete = assertInstanceOf(OpenApiMappingTurnGate.Complete.class,
            PrepareOpenApiMappingTurnService.prepare(state.retry(List.of("invalid proposal"))));
        assertFalse(complete.value().valid());
        assertEquals(List.of("invalid proposal"), complete.value().diagnostics());
    }

}
