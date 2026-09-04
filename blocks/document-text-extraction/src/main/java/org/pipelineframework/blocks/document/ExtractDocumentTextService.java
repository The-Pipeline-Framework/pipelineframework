package org.pipelineframework.blocks.document;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;
import org.pipelineframework.service.ReactiveService;
import org.pipelineframework.step.NonRetryableException;

/** Packaged deterministic document extraction, explicitly offloaded from the reactive event loop. */
@ApplicationScoped
public final class ExtractDocumentTextService implements ReactiveService<MaterializedDocument, ExtractedDocument> {
    private static final DocumentTextExtractor EXTRACTOR = new DocumentTextExtractor();

    @Override
    public Uni<ExtractedDocument> process(MaterializedDocument document) {
        return Uni.createFrom().item(() -> extract(document)).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    static ExtractedDocument extract(MaterializedDocument document) {
        return extract(document, EXTRACTOR);
    }

    static ExtractedDocument extract(MaterializedDocument document, DocumentTextExtractor extractor) {
        Objects.requireNonNull(document, "materialized document must not be null");
        Objects.requireNonNull(extractor, "document text extractor must not be null");
        try {
            ExtractedText extracted = extractor.extract(new DocumentExtractionRequest(
                document.content(), document.fileName(), document.contentType()));
            DocumentExtractionDiagnostics details = extracted.diagnostics();
            return new ExtractedDocument(document.sourceId(), extracted.text(), new ExtractionDiagnostics(
                details.format().name(), details.selectedBy().name(), details.contentType(), details.inputBytes(),
                details.extractedCharacters(), details.notes()));
        } catch (IllegalArgumentException failure) {
            throw new NonRetryableException(failure.getMessage(), failure);
        }
    }
}
