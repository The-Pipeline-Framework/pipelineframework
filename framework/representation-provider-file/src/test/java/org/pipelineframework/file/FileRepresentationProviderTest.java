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
    void claimsBarePathInputOnlyBoundaryAndGeneratesSinglePayloadFacade() {
        BoundaryRequest boundary = new BoundaryRequest(
            "Read document", "example.ReadDocument", input, output, "UNARY_UNARY", Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of(),
                "inputFields", Map.of("content", "payload_ref"),
                "outputFields", Map.of("content", "payload_ref")));
        var mapping = provider.resolve(mapping(input)).orElseThrow();

        String source = provider.describeArtifacts(new ProviderGenerationRequest(
            boundary, provider.claim(boundary).orElseThrow(), List.of(mapping), Map.of("input", Map.of())))
            .getFirst().content();

        assertTrue(source.contains("orderedInputs(java.util.Map.entry(\"content\", input.content()))"));
        assertTrue(source.contains("paths -> delegate.process(paths.get(\"content\"))"));
    }

    @Test
    void escapesC0ControlsInGeneratedFileOutputLiterals() {
        BoundaryRequest boundary = boundary("UNARY_UNARY");
        var inputMapping = provider.resolve(mapping(input)).orElseThrow();
        var outputMapping = provider.resolve(mapping(output)).orElseThrow();
        String controls = "line\nreturn\rtab\tback\bform\fnull\0vertical\013end";

        String source = provider.describeArtifacts(new ProviderGenerationRequest(
            boundary, provider.claim(boundary).orElseThrow(), List.of(inputMapping, outputMapping),
            Map.of("input", Map.of(), "output", Map.of("target", controls, "key", controls))))
            .getFirst().content();

        assertTrue(source.contains("line\\nreturn\\rtab\\tback\\bform\\fnull\\000vertical\\013end"));
        assertTrue(!source.contains("\"line\nreturn"));
        assertTrue(!source.contains("\r"));
        assertTrue(!source.contains("\t"));
        assertTrue(!source.contains("\b"));
        assertTrue(!source.contains("\f"));
        assertTrue(!source.contains("\0"));
        assertTrue(!source.contains(String.valueOf((char) 0x0b)));
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

    @Test
    void materializesOnlyTheExplicitPayloadSubset() {
        CanonicalType files = new CanonicalType("Files", "example.Files", CanonicalTypeShape.RECORD);
        BoundaryRequest boundary = structuredBoundary(files, "UNARY_UNARY");
        var mapping = provider.resolve(new RepresentationMappingRequest(
            "file", files, Optional.of("example.MaterializedFiles"), Optional.empty(),
            Map.of("fields", List.of("invoice", "catalogue"), "materializeFields", List.of("invoice"))))
            .orElseThrow();

        String source = provider.describeArtifacts(new ProviderGenerationRequest(boundary,
            provider.claim(boundary).orElseThrow(), List.of(mapping), Map.of("input", Map.of(
                "fields", List.of("invoice", "catalogue"), "materializeFields", List.of("invoice")))))
            .getFirst().content();

        assertTrue(source.contains("paths.get(\"invoice\")"));
        assertTrue(source.contains("input.catalogue()"));
        assertTrue(!source.contains("Map.entry(\"catalogue\""));
    }

    @Test
    void rejectsInvalidMaterializeFieldsAndJavaFieldNames() {
        CanonicalType files = new CanonicalType("Files", "example.Files", CanonicalTypeShape.RECORD);
        BoundaryRequest boundary = structuredBoundary(files, "UNARY_UNARY");
        var mapping = provider.resolve(new RepresentationMappingRequest(
            "file", files, Optional.of("example.MaterializedFiles"), Optional.empty(),
            Map.of("fields", List.of("invoice", "catalogue")))).orElseThrow();
        var claim = provider.claim(boundary).orElseThrow();

        IllegalStateException nonList = assertThrows(IllegalStateException.class, () -> provider.describeArtifacts(
            new ProviderGenerationRequest(boundary, claim, List.of(mapping), Map.of("input", Map.of(
                "fields", List.of("invoice", "catalogue"), "materializeFields", "invoice")))));
        assertTrue(nonList.getMessage().contains("non-empty string list"));
        IllegalStateException nonPayload = assertThrows(IllegalStateException.class, () -> provider.describeArtifacts(
            new ProviderGenerationRequest(boundary, claim, List.of(mapping), Map.of("input", Map.of(
                "fields", List.of("invoice", "catalogue"), "materializeFields", List.of("missing"))))));
        assertTrue(nonPayload.getMessage().contains("payload_ref fields"));

        BoundaryRequest keywordBoundary = new BoundaryRequest("Prepare", "example.Prepare", files, output,
            "UNARY_UNARY", Set.of(), Map.of("inputMappings", List.of("file"), "outputMappings", List.of(),
                "inputFields", Map.of("class", "payload_ref"), "outputFields", Map.of("content", "payload_ref")));
        var keywordMapping = provider.resolve(new RepresentationMappingRequest("file", files,
            Optional.of("java.nio.file.Path"), Optional.empty(), Map.of("fields", List.of("class")))).orElseThrow();
        IllegalStateException keyword = assertThrows(IllegalStateException.class, () -> provider.describeArtifacts(
            new ProviderGenerationRequest(keywordBoundary, provider.claim(keywordBoundary).orElseThrow(),
                List.of(keywordMapping), Map.of("input", Map.of("fields", List.of("class"))))));
        assertTrue(keyword.getMessage().contains("valid Java identifier"));
    }

    @Test
    void rejectsInputOnlyOneToManyBoundary() {
        CanonicalType files = new CanonicalType("Files", "example.Files", CanonicalTypeShape.RECORD);
        IllegalStateException failure = assertThrows(IllegalStateException.class,
            () -> provider.claim(structuredBoundary(files, "UNARY_STREAMING")));
        assertTrue(failure.getMessage().contains("supports ONE_TO_ONE only"));
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

    private BoundaryRequest structuredBoundary(CanonicalType files, String cardinality) {
        return new BoundaryRequest("Prepare", "example.Prepare", files, output, cardinality, Set.of(), Map.of(
            "inputMappings", List.of("file"), "outputMappings", List.of(),
            "inputFields", Map.of("invoice", "payload_ref", "catalogue", "payload_ref"),
            "outputFields", Map.of("content", "payload_ref")));
    }
}
