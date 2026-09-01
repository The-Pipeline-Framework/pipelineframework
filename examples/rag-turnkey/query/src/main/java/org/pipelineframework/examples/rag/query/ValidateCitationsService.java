package org.pipelineframework.examples.rag.query;

import java.util.HashMap;
import java.util.HashSet;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.examples.rag.query.domain.Answer;
import org.pipelineframework.examples.rag.query.domain.AnswerCandidate;
import org.pipelineframework.examples.rag.query.domain.Citation;
import org.pipelineframework.examples.rag.query.domain.RetrievedChunk;
import org.pipelineframework.service.ReactiveService;

/** Treats model-authored citation IDs as claims and resolves all citation data from trusted context. */
@ApplicationScoped
public final class ValidateCitationsService implements ReactiveService<AnswerCandidate, Answer> {
    @Override public Uni<Answer> process(AnswerCandidate candidate) {
        return Uni.createFrom().item(validate(candidate));
    }

    static Answer validate(AnswerCandidate candidate) {
        var available = new HashMap<String, RetrievedChunk>();
        candidate.chunks().forEach(chunk -> available.put(chunk.chunkId(), chunk));
        var seen = new HashSet<String>();
        var citations = candidate.draft().citedChunkIds().stream().map(chunkId -> {
            if (!seen.add(chunkId)) throw new IllegalArgumentException("duplicate cited chunk ID: " + chunkId);
            RetrievedChunk trusted = available.get(chunkId);
            if (trusted == null) throw new IllegalArgumentException("answer cited an unknown chunk ID: " + chunkId);
            return new Citation(trusted.sourceId(), trusted.chunkId(), trusted.excerpt());
        }).toList();
        if (!candidate.chunks().isEmpty() && citations.isEmpty()) {
            throw new IllegalArgumentException("an answer with retrieved context must cite at least one chunk");
        }
        return new Answer(candidate.draft().text(), citations);
    }
}
