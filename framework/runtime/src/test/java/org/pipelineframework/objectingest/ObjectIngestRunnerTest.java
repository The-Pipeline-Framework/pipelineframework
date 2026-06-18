package org.pipelineframework.objectingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineInputBoundaryConfig;
import org.pipelineframework.config.boundary.PipelineObjectInputConfig;
import org.pipelineframework.config.boundary.PipelineObjectSourceConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.orchestrator.dto.RunAsyncAcceptedDto;

class ObjectIngestRunnerTest {

    @Test
    void pollOnceMapsSnapshotsAndSubmitsWithStableObjectIdentity() {
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "documents",
            "object",
            "test",
            Map.of(),
            null,
            null,
            null,
            null);
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.pipelineframework.objectingest",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of("documents", source),
            List.of(),
            new PipelineInputBoundaryConfig(null, new PipelineObjectInputConfig(
                "documents",
                TestInput.class.getName(),
                "TestInput",
                TestMapper.class.getName())),
            null);
        List<String> keys = new ArrayList<>();
        List<Object> inputs = new ArrayList<>();
        ObjectIngestRunner runner = new ObjectIngestRunner(
            config,
            new ObjectSourceRegistry(List.of(new TestProvider())),
            (input, tenantId, idempotencyKey) -> {
                inputs.add(input);
                keys.add(idempotencyKey);
                return Uni.createFrom().item(new RunAsyncAcceptedDto("execution-1", false, "/executions/execution-1", 1L));
            },
            ObjectIngestTelemetry.NOOP);

        ObjectIngestRunner.PollResult result = runner.pollOnce();

        assertEquals(1, result.listed());
        assertEquals(1, result.submitted());
        assertEquals(0, result.failed());
        assertEquals("alpha.txt", ((TestInput) inputs.getFirst()).key());
        assertEquals("object:documents:test:bucket:alpha.txt:v1:etag-1", keys.getFirst());
    }

    @Test
    void pollOnceRejectsMapperOutsidePipelineBasePackage() {
        PipelineObjectSourceConfig source = new PipelineObjectSourceConfig(
            "documents",
            "object",
            "test",
            Map.of(),
            null,
            null,
            null,
            null);
        PipelineYamlConfig config = new PipelineYamlConfig(
            "org.pipelineframework.objectingest",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of("documents", source),
            List.of(),
            new PipelineInputBoundaryConfig(null, new PipelineObjectInputConfig(
                "documents",
                TestInput.class.getName(),
                "TestInput",
                "com.example.NotAllowedMapper")),
            null);
        List<Object> inputs = new ArrayList<>();
        ObjectIngestRunner runner = new ObjectIngestRunner(
            config,
            new ObjectSourceRegistry(List.of(new TestProvider())),
            (input, tenantId, idempotencyKey) -> {
                inputs.add(input);
                return Uni.createFrom().item(new RunAsyncAcceptedDto("execution-1", false, "/executions/execution-1", 1L));
            },
            ObjectIngestTelemetry.NOOP);

        ObjectIngestRunner.PollResult result = runner.pollOnce();

        assertEquals(1, result.listed());
        assertEquals(0, result.submitted());
        assertEquals(1, result.failed());
        assertEquals(List.of(), inputs);
    }

    @Test
    void executionKeyEscapesSourceName() {
        ObjectSnapshot snapshot = new ObjectSnapshot(
            "documents",
            "test",
            "bucket",
            "alpha.txt",
            "v1",
            "etag-1",
            12L,
            100L,
            "text/plain",
            Map.of(),
            null,
            null,
            null);

        assertEquals(
            "object:documents\\:raw:test:bucket:alpha.txt:v1:etag-1",
            ObjectIdentity.executionKey("documents:raw", snapshot, null));
    }

    @Test
    void sourceRegistryRejectsDuplicateProviderNames() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new ObjectSourceRegistry(List.of(new TestProvider(), new DuplicateTestProvider())));

        assertEquals("Duplicate object source provider: test", exception.getMessage());
    }

    @Test
    void objectSnapshotRejectsNullMetadataValuesWithClearMessage() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("owner", null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ObjectSnapshot(
            "documents",
            "test",
            "bucket",
            "alpha.txt",
            "v1",
            "etag-1",
            12L,
            100L,
            "text/plain",
            metadata,
            null,
            null,
            null));

        assertEquals("object snapshot metadata must not contain null keys or values", exception.getMessage());
    }

    @Test
    void objectSourceItemRejectsNullMetadataKeysWithClearMessage() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put(null, "owner");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new ObjectSourceItem(
            "test",
            "bucket",
            "alpha.txt",
            "v1",
            "etag-1",
            12L,
            100L,
            "text/plain",
            metadata,
            null,
            null));

        assertEquals("object source item metadata must not contain null keys or values", exception.getMessage());
    }

    public static final class TestMapper implements ObjectSnapshotMapper<TestInput> {
        @Override
        public TestInput map(ObjectSnapshot snapshot) {
            return new TestInput(snapshot.key());
        }
    }

    record TestInput(String key) {
    }

    private static final class TestProvider implements ObjectSourceProvider {
        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
            return List.of(new ObjectSourceItem(
                "test",
                "bucket",
                "alpha.txt",
                "v1",
                "etag-1",
                12L,
                100L,
                "text/plain",
                Map.of(),
                null,
                null));
        }
    }

    private static final class DuplicateTestProvider implements ObjectSourceProvider {
        @Override
        public String providerName() {
            return " TEST ";
        }

        @Override
        public List<ObjectSourceItem> list(PipelineObjectSourceConfig source, int limit) {
            return List.of();
        }
    }
}
