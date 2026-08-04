package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectIdentityConfig;
import org.pipelineframework.config.boundary.PipelineObjectPayloadConfig;
import org.pipelineframework.config.boundary.PipelineObjectPollConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;

class StdioObjectSourceProviderTest {

    @Test
    void readsOneEofDelimitedObjectAndThenBecomesExhausted() {
        String json = "{\"name\":\"Mariano\"}";
        StdioObjectSourceProvider provider = provider(json);
        PipelineObjectSourceConfig source = source(Map.of("endpoint", "stdin"), textPayload());

        List<ObjectSourceItem> items = provider.list(source, 1);

        assertEquals(1, items.size());
        ObjectSourceItem item = items.getFirst();
        assertEquals("stdio", item.provider());
        assertEquals("stdin", item.key());
        assertNotNull(item.etag());
        assertEquals(json, provider.readText(source, item, 1024).orElseThrow());
        assertEquals(List.of(), provider.list(source, 1));
    }

    @Test
    void preservesJsonCollectionAsOneObjectForTheExistingMapper() {
        String collection = "[{\"name\":\"Mariano\"},{\"name\":\"Ada\"}]";
        StdioObjectSourceProvider provider = provider(collection);
        PipelineObjectSourceConfig source = source(Map.of("endpoint", "stdin"), textPayload());

        ObjectSourceItem item = provider.list(source, 1).getFirst();

        assertEquals(collection, provider.readText(source, item, 1024).orElseThrow());
        assertEquals(List.of(), provider.list(source, 1));
    }

    @Test
    void rejectsNonTextAndFilesystemStyleConfiguration() {
        StdioObjectSourceProvider provider = provider("{}");

        assertThrows(IllegalArgumentException.class, () -> provider.list(
            source(Map.of("endpoint", "stdin"), PipelineObjectPayloadConfig.reference()), 1));
        assertThrows(IllegalArgumentException.class, () -> provider.list(
            source(Map.of("endpoint", "stdin", "root", "/tmp"), textPayload()), 1));
    }

    private StdioObjectSourceProvider provider(String input) {
        return new StdioObjectSourceProvider(new StandardStreams(
            new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), new ByteArrayOutputStream()));
    }

    private PipelineObjectSourceConfig source(Map<String, Object> location, PipelineObjectPayloadConfig payload) {
        return new PipelineObjectSourceConfig(
            "stdin-input",
            "object",
            "stdio",
            location,
            PipelineObjectFilterConfig.defaults(),
            PipelineObjectPollConfig.defaults(),
            PipelineObjectIdentityConfig.defaults(),
            payload);
    }

    private PipelineObjectPayloadConfig textPayload() {
        return new PipelineObjectPayloadConfig("text", "", 1024L, StandardCharsets.UTF_8);
    }
}
