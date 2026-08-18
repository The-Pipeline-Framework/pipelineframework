package org.pipelineframework.file;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void doesNotClaimOnlyOneMappedSide() {
        BoundaryRequest boundary = new BoundaryRequest(
            "Render", "example.RenderService", input, output, "UNARY_UNARY", Set.of(),
            Map.of(
                "inputMappings", List.of("file"),
                "outputMappings", List.of(),
                "inputFields", Map.of("content", "payload_ref"),
                "outputFields", Map.of("content", "payload_ref")));

        assertTrue(provider.claim(boundary).isEmpty());
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
