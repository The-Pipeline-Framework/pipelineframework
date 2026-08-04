package org.pipelineframework.connector.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.boundary.PipelineObjectFilterConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.objectingest.ObjectSourceItem;

class FilesystemObjectSourceProviderTest {

    @TempDir
    Path tempDir;

    @Test
    void listsMatchingFilesWithPayloadReferences() throws Exception {
        Files.writeString(tempDir.resolve("payments.csv"), "id,amount\n1,10\n");
        Files.writeString(tempDir.resolve("ignored.txt"), "ignore");
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "csv-payment-files",
            "object",
            "filesystem",
            Map.of("root", tempDir.toString()),
            new PipelineObjectFilterConfig(List.of("*.csv"), List.of()),
            null,
            null,
            null);

        List<ObjectSourceItem> items = new FilesystemObjectSourceProvider().list(source, 10);

        assertEquals(1, items.size());
        ObjectSourceItem item = items.getFirst();
        assertEquals("payments.csv", item.key());
        assertEquals("filesystem", item.provider());
        assertNotNull(item.etag());
        assertNotNull(item.contentRef());
        assertEquals(tempDir.toString(), item.contentRef().container());
    }

    @Test
    void rejectsOversizedAndEscapedReads() throws Exception {
        Files.writeString(tempDir.resolve("big.csv"), "id,amount\n1,100\n");
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "csv-payment-files",
            "object",
            "filesystem",
            Map.of("root", tempDir.toString()),
            null,
            null,
            null,
            null);
        FilesystemObjectSourceProvider provider = new FilesystemObjectSourceProvider();

        assertThrows(IllegalStateException.class, () -> provider.readText(source, item("big.csv"), 3L));
        assertThrows(SecurityException.class, () -> provider.readText(source, item("../outside.csv"), 0L));
    }

    private ObjectSourceItem item(String key) {
        return new ObjectSourceItem(
            "filesystem",
            tempDir.toString(),
            key,
            null,
            "etag",
            1L,
            1L,
            "text/csv",
            Map.of(),
            null,
            tempDir.resolve(key).normalize().toString());
    }
}
