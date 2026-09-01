package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.embedding.EmbeddingResult;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchResult;

class ContributedProtocolQueueCodecTest {
    private final JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();

    @Test
    void repeatedEmbeddingValuesRoundTripWithoutLosingOrderOrDuplicates() {
        EmbeddingResult result = new EmbeddingResult("chunk-1", "content", List.of(0.5f, -0.25f, 0.5f));

        assertEquals(result, codec.decode(codec.encode(result)));
    }

    @Test
    void nestedRepeatedMatchesRoundTripWithoutLosingOrderOrDuplicates() {
        VectorMatch first = new VectorMatch("a", "alpha", 0.75f);
        VectorMatch second = new VectorMatch("b", "beta", 0.5f);
        VectorSearchResult result = new VectorSearchResult(
            "question-1", "question", List.of(first, first, second));

        assertEquals(result, codec.decode(codec.encode(result)));
    }
}
