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
    
    /**
     * Splits a document's text into fixed-size word chunks and emits them as a stream.
     *
     * If the input is null, or its content is null or empty after trimming, an empty stream is returned.
     *
     * Each resulting Chunk contains up to 10 words. Chunk fields are set as follows:
     * - id: "{documentId}_chunk_{position}"
     * - documentId: the input document's id
     * - content: the chunk's text
     * - position: zero-based chunk index
     *
     * @param input the document to chunk; may be null
     * @return a Multi emitting the generated Chunk instances, or an empty Multi if the input has no content
     */
    @Override
    public Multi<Chunk> process(Document input) {
        if (input == null || input.content() == null || input.id() == null) {
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
     * Produce chunk DTOs from a DocumentDto by splitting its content into chunks.
     *
     * If the input or its content is null or empty, no chunks are produced.
     *
     * @param input the document DTO to convert and chunk
     * @return ChunkDto objects representing the sequential chunks of the document
     */
    public Multi<ChunkDto> processDto(DocumentDto input) {
        if (input == null) {
            return Multi.createFrom().empty();
        }
        Document entity = new Document(input.id(), input.content());
        return process(entity).map(chunk -> new ChunkDto(chunk.id(), chunk.documentId(), chunk.content(), chunk.position()));
    }
}