package org.pipelineframework.examples.ragproof;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.smallrye.mutiny.Multi;
import jakarta.enterprise.context.ApplicationScoped;

import org.pipelineframework.examples.ragproof.domain.Chunk;
import org.pipelineframework.examples.ragproof.domain.Document;
import org.pipelineframework.service.ReactiveStreamingService;

/** Application-authored deterministic fixed-window chunking. */
@ApplicationScoped
public class ChunkDocumentService implements ReactiveStreamingService<Document, Chunk> {
    private static final int WINDOW_WORDS = 6;

    @Override
    public Multi<Chunk> process(Document document) {
        List<String> words = Arrays.stream(document.text().trim().split("\\s+")).filter(word -> !word.isBlank()).toList();
        List<Chunk> chunks = new ArrayList<>();
        for (int start = 0, index = 0; start < words.size(); start += WINDOW_WORDS, index++) {
            String content = String.join(" ", words.subList(start, Math.min(start + WINDOW_WORDS, words.size())));
            chunks.add(new Chunk(document.documentId() + "#" + String.format("%04d", index), content));
        }
        return Multi.createFrom().iterable(chunks);
    }
}
