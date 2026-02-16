package com.example.ai.sdk.service;

import com.example.ai.sdk.dto.DocumentDto;
import com.example.ai.sdk.dto.ChunkDto;
import com.example.ai.sdk.entity.Document;
import com.example.ai.sdk.entity.Chunk;
import io.smallrye.mutiny.Multi;
import org.pipelineframework.service.ReactiveStreamingService;

import java.util.Arrays;

/**
 * Document chunking service that splits documents into chunks.
 * Implements Uni -> Multi cardinality (UnaryMany).
 */
public class DocumentChunkingService implements ReactiveStreamingService<Document, Chunk> {
    
    @Override
    public Multi<Chunk> process(Document input) {
        if (input == null || input.content() == null) {
            return Multi.createFrom().empty();
        }

        String trimmedContent = input.content().trim();
        if (trimmedContent.isEmpty()) {
            return Multi.createFrom().empty();
        }

        // Simple chunking logic: split by spaces every 10 words
        String[] words = trimmedContent.split("\\s+");
        int chunkSize = 10;
        int numChunks = (int) Math.ceil((double) words.length / chunkSize);
        
        Chunk[] chunks = new Chunk[numChunks];
        for (int i = 0; i < numChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, words.length);
            
            String chunkContent = String.join(" ", Arrays.copyOfRange(words, start, end));

            chunks[i] = new Chunk(
                input.id() + "_chunk_" + i,
                input.id(),
                chunkContent,
                i
            );
        }
        
        return Multi.createFrom().iterable(Arrays.asList(chunks));
    }
    
    /**
     * Alternative method that accepts DTOs directly for TPF delegation
     */
    public Multi<ChunkDto> processDto(DocumentDto input) {
        Document entity = new Document(input.id(), input.content());
        return process(entity).map(chunk -> new ChunkDto(chunk.id(), chunk.documentId(), chunk.content(), chunk.position()));
    }
}
