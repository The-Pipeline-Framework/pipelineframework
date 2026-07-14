package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectIdentityConfig;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectPollConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteSession;

class StdioObjectPipelineSmokeTest {

    @Test
    void mapsOneJsonObjectAndLeavesStdoutMachineReadable() {
        assertPipedOutput("{\"name\":\"Mariano\"}");
    }

    @Test
    void mapsOneJsonCollectionWhenTheContractIsACollection() {
        assertPipedOutput("[{\"name\":\"Mariano\"},{\"name\":\"Ada\"}]");
    }

    @Test
    void malformedJsonFailsBeforeAnythingIsWrittenToStdout() {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        StandardStreams streams = new StandardStreams(
            new ByteArrayInputStream("not-json".getBytes(StandardCharsets.UTF_8)), stdout);
        StdioObjectSourceProvider sourceProvider = new StdioObjectSourceProvider(streams);
        ObjectSourceItem item = sourceProvider.list(source(), 1).getFirst();

        assertThrows(IllegalArgumentException.class, () -> JsonContract.map(
            sourceProvider.readText(source(), item, 1024L).orElseThrow()));
        assertEquals("", stdout.toString(StandardCharsets.UTF_8));
    }

    private void assertPipedOutput(String input) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        StandardStreams streams = new StandardStreams(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), stdout);
        StdioObjectSourceProvider sourceProvider = new StdioObjectSourceProvider(streams);
        ObjectSourceItem item = sourceProvider.list(source(), 1).getFirst();
        String typedContractValue = JsonContract.map(sourceProvider.readText(source(), item, 1024L).orElseThrow());

        StdioObjectTargetProvider targetProvider = new StdioObjectTargetProvider(streams);
        ObjectWriteSession session = targetProvider.open(new ObjectWriteOpenRequest(
            "stdout-output", target(), "result.json", "application/json", Map.of(), "smoke"))
            .toCompletableFuture().join();
        byte[] bytes = typedContractValue.getBytes(StandardCharsets.UTF_8);
        session.write(ByteBuffer.wrap(bytes)).toCompletableFuture().join();
        session.close(new ObjectWriteCloseRequest(bytes.length, "smoke", Map.of("records", "1"))).toCompletableFuture().join();

        assertEquals(input, stdout.toString(StandardCharsets.UTF_8));
    }

    private PipelineObjectSourceConfig source() {
        return new PipelineObjectSourceConfig(
            "stdin-input", "object", "stdio", Map.of("endpoint", "stdin"),
            PipelineObjectFilterConfig.defaults(), PipelineObjectPollConfig.defaults(), PipelineObjectIdentityConfig.defaults(),
            new PipelineObjectPayloadConfig("text", "", 1024L, StandardCharsets.UTF_8));
    }

    private PipelineObjectPublishConfig target() {
        return new PipelineObjectPublishConfig(
            "stdout-output", "object", "stdio", Map.of("endpoint", "stdout"),
            PipelineObjectNamingConfig.defaults(), PipelineObjectPublishPayloadConfig.defaults(), PipelineObjectPublishGroupingConfig.defaults());
    }

    private static final class JsonContract {
        private static String map(String content) {
            String value = content.trim();
            boolean object = value.startsWith("{") && value.endsWith("}");
            boolean collection = value.startsWith("[") && value.endsWith("]");
            if (!object && !collection) {
                throw new IllegalArgumentException("Expected one JSON object or JSON collection");
            }
            return content;
        }
    }
}
