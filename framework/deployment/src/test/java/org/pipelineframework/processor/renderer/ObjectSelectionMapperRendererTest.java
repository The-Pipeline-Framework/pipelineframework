package org.pipelineframework.processor.renderer;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.squareup.javapoet.ClassName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectSelectionConfig;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.processor.ir.DeploymentRole;

class ObjectSelectionMapperRendererTest {
    @TempDir
    Path output;

    @Test
    void rendersExplicitKeyProjectionInCanonicalFieldOrder() throws Exception {
        var record = new PipelineTemplateTypeDefinition.RecordType("Documents", List.of(
            field("invoice"), field("attachment")));
        var selection = new PipelineObjectSelectionConfig("together",
            Map.of("invoice", "invoice.pdf", "attachment", "attachment.pdf"), Optional.empty());

        new ObjectSelectionMapperRenderer().render(
            "com.example",
            ClassName.get("com.example.domain", "Documents"),
            record,
            selection,
            new GenerationContext(null, output, DeploymentRole.PIPELINE_SERVER, Set.of(), null, null));

        String source = Files.readString(output.resolve(
            "com/example/pipeline/ObjectSelectionPipelineInputMapper.java"));
        assertTrue(source.contains("return new Documents(reference(snapshots, \"invoice.pdf\"), reference(snapshots, \"attachment.pdf\"))"));
        assertTrue(Files.readString(output.resolve(
            "META-INF/services/org.pipelineframework.objectingest.ObjectSelectionMapper"))
            .contains("com.example.pipeline.ObjectSelectionPipelineInputMapper"));
    }

    @Test
    void rendersHomogeneousRepeatedProjectionWithoutDefiningSingletonFailure() throws Exception {
        var record = new PipelineTemplateTypeDefinition.RecordType("Documents", List.of(field("documents")));
        var selection = new PipelineObjectSelectionConfig(
            "together", Map.of(), Optional.of("documents"));

        new ObjectSelectionMapperRenderer().render(
            "com.example",
            ClassName.get("com.example.domain", "Documents"),
            record,
            selection,
            new GenerationContext(null, output, DeploymentRole.PIPELINE_SERVER, Set.of(), null, null));

        String source = Files.readString(output.resolve(
            "com/example/pipeline/ObjectSelectionPipelineInputMapper.java"));
        assertTrue(source.contains("snapshots.stream().map(snapshot -> reference(snapshot, snapshot.key())).toList()"));
    }

    private static PipelineTemplateTypeDefinition.Field field(String name) {
        return new PipelineTemplateTypeDefinition.Field(
            name, new PipelineTemplateTypeReference.Scalar("payload_ref"));
    }
}
