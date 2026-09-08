package org.pipelineframework.examples.agentproof;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.pipelineframework.examples.agentproof.domain.OperationEmptyObservation;
import org.pipelineframework.examples.agentproof.domain.OperationObservation;

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

    private static OperationObservation emptyObservation(String contextJson) {
        return new OperationObservation.Empty(new OperationEmptyObservation(
            "proof", "lookup", "QUERY", 1, "not-found", "missing", "{}", contextJson));
    }
}
