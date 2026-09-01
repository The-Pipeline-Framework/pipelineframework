package org.pipelineframework.examples.rag.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.vector.VectorMatch;
import org.pipelineframework.connector.vector.VectorSearchResult;
import org.pipelineframework.examples.rag.query.domain.AnswerCandidate;
import org.pipelineframework.examples.rag.query.domain.AnswerDraft;
import org.pipelineframework.examples.rag.query.domain.RetrievedChunk;
import org.pipelineframework.examples.rag.support.ChunkId;

class QueryServicesTest {
    @Test void reconstructsTrustedSourceAndCitations() {
        String chunkId = ChunkId.encode("manual#v2", 0, "trusted excerpt");
        var context = new RetrievedContextService().process(new VectorSearchResult(
            "q", "question", List.of(new VectorMatch(chunkId, "trusted excerpt", 0.9f))))
            .await().indefinitely();
        var answer = ValidateCitationsService.validate(new AnswerCandidate("q", "question", context.chunks(),
            new AnswerDraft("answer", List.of(chunkId))));
        assertEquals("manual#v2", answer.citations().getFirst().sourceId());
        assertEquals("trusted excerpt", answer.citations().getFirst().excerpt());
    }

    @Test void rejectsUnknownDuplicateAndMissingCitations() {
        var chunk = new RetrievedChunk("source", "chunk", "excerpt", 1.0f);
        assertThrows(IllegalArgumentException.class, () -> ValidateCitationsService.validate(candidate(chunk, List.of("other"))));
        assertThrows(IllegalArgumentException.class, () -> ValidateCitationsService.validate(candidate(chunk, List.of("chunk", "chunk"))));
        assertThrows(IllegalArgumentException.class, () -> ValidateCitationsService.validate(candidate(chunk, List.of())));
        assertEquals(List.of(), ValidateCitationsService.validate(new AnswerCandidate(
            "q", "question", List.of(), new AnswerDraft("no evidence", List.of()))).citations());
    }

    private static AnswerCandidate candidate(RetrievedChunk chunk, List<String> cited) {
        return new AnswerCandidate("q", "question", List.of(chunk), new AnswerDraft("answer", cited));
    }
}
