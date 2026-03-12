package org.pipelineframework.processor.phase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.processor.PipelineCompilationContext;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtobufParserServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void generatedParserUsesSchemaNameWithoutLegacyTypeAliases() throws IOException {
        PipelineCompilationContext context = new PipelineCompilationContext(null, null);
        context.setGeneratedSourcesRoot(tempDir);
        ProtobufParserService service = new ProtobufParserService(new GenerationPathResolver());

        service.generateProtobufParsers(context, descriptorSet());

        Path generated = tempDir
            .resolve("pipeline-server")
            .resolve("com")
            .resolve("acme")
            .resolve("checkout")
            .resolve("pipeline")
            .resolve("ProtoOrderPlacedParser.java");
        assertTrue(Files.exists(generated));

        String source = Files.readString(generated);
        assertTrue(source.contains("return \"checkout.v1.OrderPlaced\";"));
        assertFalse(source.contains("legacyTypeAliases"));
    }

    private static DescriptorProtos.FileDescriptorSet descriptorSet() {
        DescriptorProtos.FileDescriptorProto file = DescriptorProtos.FileDescriptorProto.newBuilder()
            .setName("checkout.proto")
            .setPackage("checkout.v1")
            .setOptions(DescriptorProtos.FileOptions.newBuilder()
                .setJavaPackage("com.acme.checkout")
                .setJavaMultipleFiles(true)
                .build())
            .addMessageType(DescriptorProtos.DescriptorProto.newBuilder()
                .setName("OrderPlaced")
                .build())
            .build();
        return DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(file)
            .build();
    }
}
