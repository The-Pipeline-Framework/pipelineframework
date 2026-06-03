package org.pipelineframework.config.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.DescriptorProtos;
import org.junit.jupiter.api.Test;

class PipelineJsonTest {

    @Test
    void serializesProtobufMessagesAsStableJson() throws Exception {
        DescriptorProtos.FileDescriptorSet payload = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName("payments.proto")
                .setPackage("org.pipelineframework.csv")
                .build())
            .build();

        String json = PipelineJson.mapper().writeValueAsString(payload);

        assertTrue(json.contains("\"file\""));
        assertTrue(json.contains("\"payments.proto\""));
        assertFalse(json.contains("unknownFields"));
        assertFalse(json.contains("parserForType"));
    }

    @Test
    void valueToTreeUsesProtobufJsonFields() {
        DescriptorProtos.FileDescriptorSet payload = DescriptorProtos.FileDescriptorSet.newBuilder()
            .addFile(DescriptorProtos.FileDescriptorProto.newBuilder().setName("search.proto").build())
            .build();

        var tree = PipelineJson.mapper().valueToTree(payload);

        assertEquals("search.proto", tree.path("file").get(0).path("name").asText());
    }
}
