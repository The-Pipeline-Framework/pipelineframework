package org.pipelineframework.examples.rag.indexer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import org.pipelineframework.examples.rag.indexer.domain.MaterializedDocument;
import org.pipelineframework.examples.rag.indexer.domain.ParsedDocument;
import org.pipelineframework.service.ReactiveService;

/** Application-owned bounded UTF-8 parser. */
@ApplicationScoped
public final class ParseDocumentService implements ReactiveService<MaterializedDocument, ParsedDocument> {
    static final long MAX_BYTES = 1_048_576;

    @Override public Uni<ParsedDocument> process(MaterializedDocument document) {
        return Uni.createFrom().item(() -> parse(document)).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    static ParsedDocument parse(MaterializedDocument document) {
        try {
            long size = Files.size(document.content());
            if (size > MAX_BYTES) throw new IllegalArgumentException("document exceeds the 1 MiB parser limit");
            byte[] bytes = Files.readAllBytes(document.content());
            if (bytes.length > MAX_BYTES) throw new IllegalArgumentException("document exceeds the 1 MiB parser limit");
            String text = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString().strip();
            if (text.isEmpty()) throw new IllegalArgumentException("document text must not be blank");
            return new ParsedDocument(document.sourceId(), text);
        } catch (IOException failure) {
            throw new IllegalArgumentException("could not parse document as UTF-8", failure);
        }
    }
}
