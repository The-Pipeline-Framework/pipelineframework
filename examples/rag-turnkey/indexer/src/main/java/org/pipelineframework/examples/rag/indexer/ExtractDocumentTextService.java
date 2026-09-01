package org.pipelineframework.examples.rag.indexer;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;
import org.pipelineframework.examples.rag.document.DocumentExtractionLimitException;
import org.pipelineframework.examples.rag.document.DocumentExtractionRequest;
import org.pipelineframework.examples.rag.document.DocumentTextExtractor;
import org.pipelineframework.examples.rag.indexer.domain.ExtractionDiagnostics;
import org.pipelineframework.examples.rag.indexer.domain.MaterializedDocument;
import org.pipelineframework.examples.rag.indexer.domain.ParsedDocument;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.step.NonRetryableException;

/** Application-owned adapter from the materialized pipeline value to deterministic document extraction. */
@ApplicationScoped
public final class ExtractDocumentTextService implements ReactiveService<MaterializedDocument, ParsedDocument> {
    private static final DocumentTextExtractor EXTRACTOR = new DocumentTextExtractor();

    @Override public Uni<ParsedDocument> process(MaterializedDocument document) {
        return Uni.createFrom().item(() -> extract(document)).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    static ParsedDocument extract(MaterializedDocument document) {
        return extract(document, EXTRACTOR);
    }

    static ParsedDocument extract(MaterializedDocument document, DocumentTextExtractor extractor) {
        Objects.requireNonNull(extractor, "document text extractor must not be null");
        try {
            var extracted = extractor.extract(new DocumentExtractionRequest(
                document.content(), document.fileName(), document.contentType()));
            var details = extracted.diagnostics();
            return new ParsedDocument(document.sourceId(), extracted.text(), new ExtractionDiagnostics(
                details.format().name(), details.selectedBy().name(), details.contentType(), details.inputBytes(),
                details.extractedCharacters(), details.notes()));
        } catch (DocumentExtractionLimitException failure) {
            throw new NonRetryableException(failure.getMessage(), failure);
        }
    }
}
