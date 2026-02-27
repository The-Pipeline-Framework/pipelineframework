package com.example.ai.sdk.test;

import com.example.ai.sdk.entity.Chunk;
import com.example.ai.sdk.entity.Document;
import com.example.ai.sdk.entity.Prompt;
import com.example.ai.sdk.entity.ScoredChunk;
import com.example.ai.sdk.entity.Vector;
import com.example.ai.sdk.service.ChunkEmbeddingService;
import com.example.ai.sdk.service.DocumentChunkingService;
import com.example.ai.sdk.service.DocumentChunkingUnaryService;
import com.example.ai.sdk.service.EmbeddingService;
import com.example.ai.sdk.service.ScoredChunkPromptService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UnaryOperatorServicesTest {

    @Test
    void documentChunkingUnaryFailsWhenNoChunksGenerated() {
        DocumentChunkingUnaryService service = new DocumentChunkingUnaryService(new EmptyChunkingService());

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> service.process(new Document("doc-1", ""))
                        .await().atMost(Duration.ofSeconds(2)));

        assertEquals("No chunks generated for input document", error.getMessage());
    }

    @Test
    void documentChunkingUnaryUsesInjectedServiceAndReturnsFirstChunk() {
        Chunk expected = new Chunk("c1", "doc-1", "first", 0);
        DocumentChunkingUnaryService service = new DocumentChunkingUnaryService(new FixedChunkingService(expected));

        Chunk result = service.process(new Document("doc-1", "anything"))
                .await().atMost(Duration.ofSeconds(2));

        assertEquals(expected, result);
    }

    @Test
    void chunkEmbeddingUsesInjectedEmbeddingService() {
        Vector expected = new Vector("vec-1", List.of(1.0f, 2.0f));
        ChunkEmbeddingService service = new ChunkEmbeddingService(new FixedEmbeddingService(expected));

        Vector result = service.process(new Chunk("c1", "doc-1", "hello", 0))
                .await().atMost(Duration.ofSeconds(2));

        assertEquals(expected, result);
    }

    @Test
    void scoredChunkPromptFailsWhenChunkIdIsNull() {
        ScoredChunkPromptService service = new ScoredChunkPromptService();
        ScoredChunk input = new ScoredChunk(new Chunk(null, "doc-1", "content", 0), 0.9f);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> service.process(input).await().atMost(Duration.ofSeconds(2)));

        assertEquals("ScoredChunk, chunk id, and chunk content must be non-null", error.getMessage());
    }

    private static final class EmptyChunkingService extends DocumentChunkingService {
        @Override
        public Multi<Chunk> process(Document input) {
            return Multi.createFrom().empty();
        }
    }

    private static final class FixedChunkingService extends DocumentChunkingService {
        private final Chunk chunk;

        private FixedChunkingService(Chunk chunk) {
            this.chunk = chunk;
        }

        @Override
        public Multi<Chunk> process(Document input) {
            return Multi.createFrom().items(chunk, new Chunk("c2", "doc-1", "second", 1));
        }
    }

    private static final class FixedEmbeddingService extends EmbeddingService {
        private final Vector vector;

        private FixedEmbeddingService(Vector vector) {
            this.vector = vector;
        }

        @Override
        public Uni<Vector> process(String input) {
            return Uni.createFrom().item(vector);
        }
    }
}
