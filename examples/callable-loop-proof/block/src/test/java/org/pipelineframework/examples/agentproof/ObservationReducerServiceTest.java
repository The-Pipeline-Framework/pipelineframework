package org.pipelineframework.examples.agentproof;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.examples.agentproof.domain.OperationEmptyObservation;
import org.pipelineframework.examples.agentproof.domain.OperationObservation;
import org.pipelineframework.examples.agentproof.domain.OperationResultObservation;

class ObservationReducerServiceTest {
    private final ObservationReducerService reducer = new ObservationReducerService();

    @Test
    void rejectsMissingOrNullTrustedContextFields() {
        List<String> malformedContexts = List.of(
            "{\"state\":\"proof\",\"evidence\":\"\",\"phase\":\"action\"}",
            "{\"state\":\"proof\",\"evidence\":\"\",\"phase\":\"action\",\"nextEffectKey\":null}",
            "{\"evidence\":\"\",\"phase\":\"action\",\"nextEffectKey\":\"effect-1\"}",
            "{\"state\":\"proof\",\"evidence\":null,\"phase\":\"action\",\"nextEffectKey\":\"effect-1\"}",
            "{\"state\":\"proof\",\"evidence\":\"\",\"nextEffectKey\":\"effect-1\"}"
        );

        malformedContexts.forEach(contextJson -> assertThrows(IllegalArgumentException.class,
            () -> reducer.process(emptyObservation(contextJson)).await().atMost(Duration.ofSeconds(1))));
    }

    @Test
    void successfulLookupStillAdvancesToTheCommandPhase() {
        var observation = new OperationObservation.Result(new OperationResultObservation(
            "proof", "lookup", "QUERY", 1, "found", "found", "{}",
            "{\"state\":\"proof\",\"evidence\":\"none\",\"phase\":\"lookup\",\"nextEffectKey\":\"effect-1\"}",
            "LookupResult", "{\"evidence\":\"verified\"}"));

        var state = reducer.process(observation).await().atMost(Duration.ofSeconds(1));

        assertEquals("action", state.phase());
        assertEquals("{\"evidence\":\"verified\"}", state.evidence());
    }

    private static OperationObservation emptyObservation(String contextJson) {
        return new OperationObservation.Empty(new OperationEmptyObservation(
            "proof", "lookup", "QUERY", 1, "not-found", "missing", "{}", contextJson));
    }
}
