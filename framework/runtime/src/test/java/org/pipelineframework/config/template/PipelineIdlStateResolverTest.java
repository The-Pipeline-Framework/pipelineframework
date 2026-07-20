package org.pipelineframework.config.template;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;

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

    @Test
    void retainsBaselineGeneratedNamesForExistingV3Members() throws Exception {
        Path yaml = tempDir.resolve("retain-proto-names.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: V3
            basePackage: com.example.v3
            transport: GRPC
            types:
              Record:
                fields: [[paymentId, string]]
              Outcome:
                variants:
                  requiresReview: Record
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: Record
                output: Outcome
            """);
        PipelineIdlSnapshot baseline = new PipelineIdlSnapshot(3, "V3", "com.example.v3", Map.of(), Map.of(), Map.of(
            "Record", new PipelineIdlSnapshot.TypeSnapshot("Record", "record",
                List.of(new PipelineIdlSnapshot.TypeFieldSnapshot(17, "paymentId", "legacy_payment", "string")),
                Optional.empty(), List.of()),
            "Outcome", new PipelineIdlSnapshot.TypeSnapshot("Outcome", "union", List.of(), Optional.empty(),
                List.of(new PipelineIdlSnapshot.TypeVariantSnapshot("requiresReview", "Record", "legacy_review", 19)))), List.of());

        PipelineIdlSnapshot resolved = new PipelineIdlStateResolver().resolve(
            new PipelineTemplateConfigLoader().load(yaml), baseline, false).state();

        assertEquals("legacy_payment", resolved.types().get("Record").fields().getFirst().protoName());
        assertEquals("legacy_review", resolved.types().get("Outcome").variants().getFirst().protoName());
    }

    @Test
    void persistsWrapperConstraintsAndTreatsMissingPriorStateAsEmpty() throws Exception {
        Path yaml = tempDir.resolve("constrained-wrapper.yaml");
        Files.writeString(yaml, """
            version: 3
            appName: V3
            basePackage: com.example.v3
            transport: GRPC
            types:
              CurrencyCode:
                wraps: string
                minLength: 3
                maxLength: 3
                pattern: "[A-Z]{3}"
            steps:
              - name: process
                cardinality: ONE_TO_ONE
                input: CurrencyCode
                output: CurrencyCode
            """);

        PipelineIdlSnapshot state = new PipelineIdlStateResolver().resolve(
            new PipelineTemplateConfigLoader().load(yaml), null, true).state();
        PipelineTemplateWrapperConstraints constraints = state.types().get("CurrencyCode").constraints();

        assertEquals(3, constraints.minLength().orElseThrow());
        assertEquals("[A-Z]{3}", constraints.pattern().orElseThrow());
        assertEquals(PipelineTemplateWrapperConstraints.empty(), new PipelineIdlSnapshot.TypeSnapshot(
            "CurrencyCode", "wrapper", List.of(), Optional.of("string"), List.of()).constraints());

        PipelineIdlSnapshot olderState = PipelineJson.mapper().copy().findAndRegisterModules().readValue("""
            {"version":3,"appName":"V3","basePackage":"com.example.v3","messages":{},"unions":{},
             "types":{"CurrencyCode":{"name":"CurrencyCode","kind":"wrapper","fields":[],"target":"string",
             "variants":[],"reservedNumbers":[],"reservedNames":[]}},"steps":[]}
            """, PipelineIdlSnapshot.class);
        assertEquals(PipelineTemplateWrapperConstraints.empty(), olderState.types().get("CurrencyCode").constraints());
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
