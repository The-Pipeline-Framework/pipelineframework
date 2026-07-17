package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PipelineIdlStateResolverTest {

    @TempDir
    Path tempDir;

    @Test
    void preservesV2UnionVariantTagsWhenResolvingV3State() throws Exception {
        Path yaml = tempDir.resolve("pipeline.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: V3
            basePackage: com.example.v3
            transport: GRPC
            types:
              Approved:
                fields: [[id, uuid]]
              Outcome:
                variants:
                  approved: Approved
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Approved
                output: Outcome
            """);
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(yaml);
        PipelineIdlSnapshot baseline = new PipelineIdlSnapshot(2, "V2", "com.example.v3", Map.of(),
            Map.of("Outcome", new PipelineIdlSnapshot.UnionSnapshot("Outcome",
                List.of(new PipelineIdlSnapshot.UnionVariantSnapshot("approved", "Approved", 17)))), List.of());

        PipelineIdlSnapshot resolved = new PipelineIdlStateResolver().resolve(config, baseline, false).state();

        assertEquals(17, resolved.types().get("Outcome").variants().getFirst().number());
    }

    @Test
    void keepsV3TagsStableAcrossReorderingAndReservesRemovedProtoIdentities() throws Exception {
        Path initialYaml = tempDir.resolve("initial.yaml");
        Files.writeString(initialYaml, template("""
            fields: [[beta, string], [alpha, string]]
            """, """
            requiresReview: Record
            approved: Record
            """));
        PipelineIdlStateResolver resolver = new PipelineIdlStateResolver();
        PipelineIdlSnapshot initial = resolver.resolve(new PipelineTemplateConfigLoader().load(initialYaml), null, true).state();

        Path reorderedYaml = tempDir.resolve("reordered.yaml");
        Files.writeString(reorderedYaml, template("""
            fields: [[alpha, string], [beta, string]]
            """, """
            approved: Record
            requiresReview: Record
            """));
        PipelineIdlSnapshot reordered = resolver.resolve(new PipelineTemplateConfigLoader().load(reorderedYaml), initial, false).state();

        assertEquals(initial.types().get("Record").fields(), reordered.types().get("Record").fields());
        assertEquals(initial.types().get("Outcome").variants(), reordered.types().get("Outcome").variants());

        Path removedYaml = tempDir.resolve("removed.yaml");
        Files.writeString(removedYaml, template("fields: [[alpha, string]]", "approved: Record"));
        PipelineIdlSnapshot removed = resolver.resolve(new PipelineTemplateConfigLoader().load(removedYaml), initial, false).state();

        assertEquals(List.of(2), removed.types().get("Record").reservedNumbers());
        assertEquals(List.of("beta"), removed.types().get("Record").reservedNames());
        assertEquals(List.of(2), removed.types().get("Outcome").reservedNumbers());
        assertEquals(List.of("requires_review"), removed.types().get("Outcome").reservedNames());
    }

    @Test
    void rejectsV3FieldsThatCollideAfterProtobufNameNormalization() throws Exception {
        Path yaml = tempDir.resolve("collision.yaml");
        Files.writeString(yaml, template("fields: [[paymentId, string], [payment_id, string]]", "approved: Record"));

        IllegalStateException error = assertThrows(IllegalStateException.class,
            () -> new PipelineIdlStateResolver().resolve(new PipelineTemplateConfigLoader().load(yaml), null, true));

        assertEquals("Type 'Record' has colliding protobuf field name 'payment_id'.", error.getMessage());
    }

    private String template(String recordBody, String variants) {
        return """
            version: 3
            appName: V3
            basePackage: com.example.v3
            transport: GRPC
            types:
              Record:
            """ + indent(recordBody, 4) + """
              Outcome:
                variants:
            """ + indent(variants, 6) + """
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Record
                output: Outcome
            """;
    }

    private String indent(String value, int spaces) {
        String prefix = " ".repeat(spaces);
        return value.lines().map(line -> prefix + line).collect(java.util.stream.Collectors.joining("\n")) + "\n";
    }
}
