package org.pipelineframework.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.CanonicalType;
import org.pipelineframework.representation.spi.CanonicalTypeShape;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;
import org.pipelineframework.representation.spi.RepresentationMappingRequest;

class FileRepresentationProviderTest {
    private final FileRepresentationProvider provider = new FileRepresentationProvider();
    private final CanonicalType input = new CanonicalType("Document", "example.Document", CanonicalTypeShape.RECORD);
    private final CanonicalType output = new CanonicalType("Rendered", "example.Rendered", CanonicalTypeShape.RECORD);

    @Test
    void claimsMappedOneToManyBoundaryAndGeneratesPathFacade() {
        BoundaryRequest boundary = boundary("UNARY_STREAMING");
        var claim = provider.claim(boundary).orElseThrow();
        var inputMapping = provider.resolve(mapping(input)).orElseThrow();
        var outputMapping = provider.resolve(mapping(output)).orElseThrow();

        String source = provider.describeArtifacts(new ProviderGenerationRequest(
            boundary,
            claim,
            List.of(inputMapping, outputMapping),
            Map.of(
                "input", Map.of("maxBytes", 1024),
                "output", Map.of("target", "rendered", "maxBytes", 2048))))
            .getFirst().content();

        assertEquals("UNARY_STREAMING", claim.stepContract().orElseThrow().cardinality());
        assertTrue(source.contains("files.oneToMany(input.content(), 1024L"));
        assertTrue(source.contains("\"rendered\", 2048L, java.util.Optional.empty()"));
        assertTrue(source.contains("new example.Rendered(reference)"));
    }

    @Test
    void claimsStructuredInputOnlyBoundaryAndGeneratesTypedFacade() {
        CanonicalType invoiceFiles = new CanonicalType(
            "InvoiceFiles", "example.InvoiceFiles", CanonicalTypeShape.RECORD);
        CanonicalType analysis = new CanonicalType(
            "InvoiceAnalysisRequest", "example.InvoiceAnalysisRequest", CanonicalTypeShape.RECORD);
        BoundaryRequest boundary = new BoundaryRequest(
            "Prepare invoice analysis", "example.PrepareInvoiceAnalysis", invoiceFiles, analysis,
            "UNARY_UNARY", Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of(),
                "inputFields", Map.of(
                    "documentId", "uuid",
                    "originalFilename", "string",
                    "invoice", "payload_ref",
                    "catalogue", "payload_ref"),
                "outputFields", Map.of("documentId", "uuid")));
        var mapping = provider.resolve(new RepresentationMappingRequest(
            "file", invoiceFiles, Optional.of("example.MaterializedInvoiceFiles"), Optional.empty(),
            Map.of("fields", List.of("documentId", "originalFilename", "invoice", "catalogue")))).orElseThrow();
        var claim = provider.claim(boundary).orElseThrow();

        String source = provider.describeArtifacts(new ProviderGenerationRequest(
            boundary, claim, List.of(mapping), Map.of("input", Map.of(
                "fields", List.of("documentId", "originalFilename", "invoice", "catalogue"),
                "maxBytes", 4096))))
            .getFirst().content();

        assertTrue(source.contains("files.withMaterialized("));
        assertTrue(source.contains("java.util.Map.entry(\"invoice\", input.invoice())"));
        assertTrue(source.contains("java.util.Map.entry(\"catalogue\", input.catalogue())"));
        assertTrue(source.contains("new example.MaterializedInvoiceFiles(input.documentId(), input.originalFilename(), paths.get(\"invoice\"), paths.get(\"catalogue\"))"));
        assertTrue(source.contains("paths -> delegate.process("));
    }

    @Test
    void rejectsStructuredInputWithoutAPayloadReference() {
        CanonicalType metadata = new CanonicalType(
            "Metadata", "example.Metadata", CanonicalTypeShape.RECORD);
        BoundaryRequest boundary = new BoundaryRequest(
            "Prepare metadata", "example.PrepareMetadata", metadata, output,
            "UNARY_UNARY", Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of(),
                "inputFields", Map.of("documentId", "uuid"),
                "outputFields", Map.of("content", "payload_ref")));
        var mapping = provider.resolve(new RepresentationMappingRequest(
            "file", metadata, Optional.of("example.MaterializedMetadata"), Optional.empty(),
            Map.of("fields", List.of("documentId")))).orElseThrow();
        var claim = provider.claim(boundary).orElseThrow();

        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            provider.describeArtifacts(new ProviderGenerationRequest(
                boundary, claim, List.of(mapping),
                Map.of("input", Map.of("fields", List.of("documentId"))))));

        assertTrue(failure.getMessage().contains("at least one payload_ref"));
    }

    @Test
    void rejectsPathShortcutForStructuredInputWithSeveralFields() {
        CanonicalType invoiceFiles = new CanonicalType(
            "InvoiceFiles", "example.InvoiceFiles", CanonicalTypeShape.RECORD);
        BoundaryRequest boundary = new BoundaryRequest(
            "Prepare invoice analysis", "example.PrepareInvoiceAnalysis", invoiceFiles, output,
            "UNARY_UNARY", Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of(),
                "inputFields", Map.of("invoice", "payload_ref", "catalogue", "payload_ref"),
                "outputFields", Map.of("content", "payload_ref")));
        var mapping = provider.resolve(new RepresentationMappingRequest(
            "file", invoiceFiles, Optional.of("java.nio.file.Path"), Optional.empty(),
            Map.of("fields", List.of("invoice", "catalogue")))).orElseThrow();
        var claim = provider.claim(boundary).orElseThrow();

        IllegalStateException failure = assertThrows(IllegalStateException.class, () ->
            provider.describeArtifacts(new ProviderGenerationRequest(
                boundary, claim, List.of(mapping),
                Map.of("input", Map.of("fields", List.of("invoice", "catalogue"))))));

        assertTrue(failure.getMessage().contains("Path only when options.fields contains exactly one field"));
    }

    private BoundaryRequest boundary(String cardinality) {
        return new BoundaryRequest(
            "Render", "example.RenderService", input, output, cardinality, Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of("file"),
                "inputFields", Map.of("content", "payload_ref"),
                "outputFields", Map.of("content", "payload_ref")));
    }

    private RepresentationMappingRequest mapping(CanonicalType type) {
        return new RepresentationMappingRequest(
            "file", type, Optional.of("java.nio.file.Path"), Optional.empty(), Map.of());
    }
}
