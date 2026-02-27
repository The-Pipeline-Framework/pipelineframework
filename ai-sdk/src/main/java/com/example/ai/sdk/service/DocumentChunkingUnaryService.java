package com.example.ai.sdk.service;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Document;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.service.ReactiveService;

import java.util.Objects;

/**
 * Phase-1 unary facade for document chunking.
 * Adapts the streaming chunking service (Document -> Multi<Chunk>) to Document -> Uni<Chunk>.
 */
public class DocumentChunkingUnaryService implements ReactiveService<Document, Chunk> {

    private final DocumentChunkingService chunkingService;

    public DocumentChunkingUnaryService() {
        this(new DocumentChunkingService());
    }

    public DocumentChunkingUnaryService(DocumentChunkingService chunkingService) {
        this.chunkingService = Objects.requireNonNull(chunkingService, "chunkingService must not be null");
    }

    @Override
    public Uni<Chunk> process(Document input) {
        if (input == null) {
            throw new IllegalArgumentException("Document input must not be null");
        }
        return chunkingService.process(input)
                .collect().asList()
                .onItem().transform(chunks -> {
                    if (chunks.isEmpty()) {
                        throw new IllegalStateException("No chunks generated for input document");
                    }
                    return chunks.get(0);
                });
    }
}
