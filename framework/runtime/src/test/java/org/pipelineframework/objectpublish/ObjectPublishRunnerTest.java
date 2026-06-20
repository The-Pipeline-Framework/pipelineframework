package org.pipelineframework.objectpublish;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;

class ObjectPublishRunnerTest {

    @Test
    void publishItemsGroupsRendersAndWritesObjects() {
        PipelineObjectPublishConfig target = target(false);
        List<ObjectWriteRequest> writes = new ArrayList<>();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target, TestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(new RecordingProvider(writes))),
            ObjectPublishTelemetry.NOOP);

        runner.publishItems(List.of(new TestOutput("a", "one"), new TestOutput("a", "two"), new TestOutput("b", "three")))
            .await().indefinitely();

        assertEquals(2, writes.size());
        assertEquals("a.out", writes.get(0).objectKey());
        assertEquals("one\ntwo", new String(writes.get(0).bytes(), StandardCharsets.UTF_8));
        assertEquals("2", writes.get(0).metadata().get("count"));
        assertEquals("b.out", writes.get(1).objectKey());
        assertEquals("object-publish:results:a.out:" + writes.get(0).checksum(), writes.get(0).idempotencyKey());
    }

    @Test
    void publishItemsSkipsEmptyOutput() {
        List<ObjectWriteRequest> writes = new ArrayList<>();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), TestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(new RecordingProvider(writes))),
            ObjectPublishTelemetry.NOOP);

        runner.publishItems(List.of()).await().indefinitely();

        assertEquals(List.of(), writes);
    }

    @Test
    void publishItemsRejectsMapperOutsideBasePackage() {
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), "com.example.NotAllowedMapper"),
            new ObjectTargetRegistry(List.of(new RecordingProvider(new ArrayList<>()))),
            ObjectPublishTelemetry.NOOP);

        assertThrows(IllegalStateException.class, () ->
            runner.publishItems(List.of(new TestOutput("a", "one"))).await().indefinitely());
    }

    @Test
    void publishItemsRejectsMalformedMapperClassName() {
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), "org.pipelineframework.objectpublish..BadMapper"),
            new ObjectTargetRegistry(List.of(new RecordingProvider(new ArrayList<>()))),
            ObjectPublishTelemetry.NOOP);

        assertThrows(IllegalStateException.class, () ->
            runner.publishItems(List.of(new TestOutput("a", "one"))).await().indefinitely());
    }

    @Test
    void publishItemsPropagatesProviderFailure() {
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(true), TestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(new FailingProvider())),
            ObjectPublishTelemetry.NOOP);

        assertThrows(RuntimeException.class, () ->
            runner.publishItems(List.of(new TestOutput("a", "one"))).await().indefinitely());
    }

    @Test
    void targetRegistryRejectsDuplicateProviderNames() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () ->
            new ObjectTargetRegistry(List.of(new RecordingProvider(new ArrayList<>()), new DuplicateProvider())));

        assertEquals("Duplicate object target provider: test", exception.getMessage());
    }

    private PipelineYamlConfig config(PipelineObjectPublishConfig target, String mapper) {
        return new PipelineYamlConfig(
            "org.pipelineframework.objectpublish",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of(),
            Map.of("results", target),
            List.of(),
            null,
            new PipelineOutputBoundaryConfig(null, new PipelineObjectOutputConfig(
                "results",
                TestOutput.class.getName(),
                "TestOutput",
                mapper)));
    }

    private PipelineObjectPublishConfig target(boolean failing) {
        return new PipelineObjectPublishConfig(
            "results",
            "object",
            failing ? "failing" : "test",
            Map.of(),
            new PipelineObjectNamingConfig("{groupKey}.out"),
            null);
    }

    public static final class TestMapper implements ObjectPublishMapper<TestOutput> {
        @Override
        public String groupKey(TestOutput item) {
            return item.group();
        }

        @Override
        public ObjectPayload render(String groupKey, List<TestOutput> items) {
            String body = String.join("\n", items.stream().map(TestOutput::value).toList());
            return new ObjectPayload(
                body.getBytes(StandardCharsets.UTF_8),
                "text/plain",
                Map.of("count", String.valueOf(items.size())));
        }
    }

    record TestOutput(String group, String value) {
    }

    private static final class RecordingProvider implements ObjectTargetProvider {
        private final List<ObjectWriteRequest> writes;

        private RecordingProvider(List<ObjectWriteRequest> writes) {
            this.writes = writes;
        }

        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public Uni<ObjectWriteResult> write(ObjectWriteRequest request) {
            writes.add(request);
            return Uni.createFrom().item(new ObjectWriteResult(null, request.bytes().length, request.checksum(), null));
        }
    }

    private static final class FailingProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return "failing";
        }

        @Override
        public Uni<ObjectWriteResult> write(ObjectWriteRequest request) {
            return Uni.createFrom().failure(new IllegalStateException("publish failed"));
        }
    }

    private static final class DuplicateProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return " TEST ";
        }

        @Override
        public Uni<ObjectWriteResult> write(ObjectWriteRequest request) {
            return Uni.createFrom().item(new ObjectWriteResult(null, 0, null, null));
        }
    }
}
