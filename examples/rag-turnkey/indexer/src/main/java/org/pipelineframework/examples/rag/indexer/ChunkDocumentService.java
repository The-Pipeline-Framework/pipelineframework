package org.pipelineframework.examples.rag.indexer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.examples.rag.indexer.domain.Chunk;
import org.pipelineframework.segments.document.ExtractedDocument;
import org.pipelineframework.examples.rag.support.ChunkId;
import org.pipelineframework.service.ReactiveStreamingService;

/** Application-authored deterministic fixed-window chunker. */
@ApplicationScoped
public final class ChunkDocumentService implements ReactiveStreamingService<ExtractedDocument, Chunk> {
    static final int WINDOW_WORDS = 120;

    @Override public Multi<Chunk> process(ExtractedDocument document) {
        return Multi.createFrom().iterable(chunks(document));
    }

    static List<Chunk> chunks(ExtractedDocument document) {
        List<String> words = Arrays.stream(document.text().strip().split("\\s+"))
            .filter(word -> !word.isBlank()).toList();
        if (words.isEmpty()) throw new IllegalArgumentException("document must contain text to chunk");
        List<Chunk> chunks = new ArrayList<>();
        for (int start = 0, index = 0; start < words.size(); start += WINDOW_WORDS, index++) {
            String content = String.join(" ", words.subList(start, Math.min(start + WINDOW_WORDS, words.size())));
            chunks.add(new Chunk(document.sourceId(), ChunkId.encode(document.sourceId(), index, content), content, index));
        }
        return List.copyOf(chunks);
    }
}
