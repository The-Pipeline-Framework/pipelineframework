package org.pipelineframework.processor.phase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.squareup.javapoet.ClassName;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.RepresentationMapping;
import org.pipelineframework.representation.spi.BoundaryClaim;
import org.pipelineframework.representation.spi.RepresentationScope;

class RepresentationProviderPreparationPhaseTest {

    @TempDir Path tempDir;

    @Test
    void claimedBoundaryValidatesItsDeclaredTypeMappingWithoutGlobalConfiguration() {
        RepresentationMapping mapping = new RepresentationMapping("opencsv", "PaymentRecord",
            Optional.of("example.PaymentRow"), Optional.of("example.PaymentMapper"), Map.of("separator", ","));

        var configuration = RepresentationProviderPreparationPhase.typeConfiguration(
            new BoundaryClaim("opencsv", "Read Payments", "example.PaymentReader", Optional.empty()), mapping);

        assertEquals(RepresentationScope.TYPE, configuration.scope());
        assertEquals("opencsv", configuration.providerKey());
        assertEquals(Map.of("separator", ","), configuration.options());
    }

    @Test
    void resolvesCanonicalOwnerFromDeclaredJavaRepresentation() throws Exception {
        var config = load("""
            version: 3
            appName: File representation
            basePackage: org.pipelineframework
            types:
              Document:
                fields: [[sourceId, string], [content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
            """);

        var canonical = RepresentationProviderPreparationPhase.canonical(
            config, ClassName.get("example", "MaterializedDocument"));

        assertEquals("Document", canonical.name());
        assertEquals("org.pipelineframework.domain.Document", canonical.targetTypeName());
        assertEquals(org.pipelineframework.representation.spi.CanonicalTypeShape.RECORD, canonical.shape());
    }

    @Test
    void rejectsAmbiguousCanonicalOwnersForOneJavaRepresentation() throws Exception {
        var config = load("""
            version: 3
            appName: Ambiguous representation
            basePackage: org.pipelineframework
            types:
              First:
                fields: [[content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
              Second:
                fields: [[content, payload_ref]]
                mappings:
                  file: { type: example.MaterializedDocument }
            """);

        assertThrows(IllegalStateException.class, () -> RepresentationProviderPreparationPhase.canonical(
            config, ClassName.get("example", "MaterializedDocument")));
    }

    private org.pipelineframework.config.template.PipelineTemplateConfig load(String yaml) throws Exception {
        Path path = tempDir.resolve("pipeline.yaml");
        Files.writeString(path, yaml);
        return new PipelineTemplateConfigLoader().load(path);
    }
}
