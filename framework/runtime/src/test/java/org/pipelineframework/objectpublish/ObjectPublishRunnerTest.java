package org.pipelineframework.objectpublish;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.subscription.MultiEmitter;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
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
    void publishMultiRequiresStreamingMapper() {
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), TestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(new RecordingProvider(new ArrayList<>()))),
            ObjectPublishTelemetry.NOOP);

        assertThrows(IllegalStateException.class, () ->
            runner.publish(Multi.createFrom().items(new TestOutput("a", "one"))));
    }

    @Test
    void publishMultiStreamsGroupedWrites() {
        StreamingRecordingProvider provider = new StreamingRecordingProvider();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), StreamingTestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(provider)),
            ObjectPublishTelemetry.NOOP);

        @SuppressWarnings("unchecked")
        Multi<TestOutput> result = (Multi<TestOutput>) runner.publish(Multi.createFrom().items(
            new TestOutput("a", "one"),
            new TestOutput("a", "two")));

        List<TestOutput> emitted = result.collect().asList().await().indefinitely();

        assertEquals(List.of(new TestOutput("a", "one"), new TestOutput("a", "two")), emitted);
        assertEquals(List.of("a.out"), provider.sessions.keySet().stream().toList());
        assertEquals("one\ntwo\n", provider.sessions.get("a.out").body());
        assertEquals("2", provider.sessions.get("a.out").closeMetadata.get("recordCount"));
    }

    @Test
    void publishMultiEmitsItemBeforeUpstreamCompletionAfterChunkWrite() {
        StreamingRecordingProvider provider = new StreamingRecordingProvider();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false), StreamingTestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(provider)),
            ObjectPublishTelemetry.NOOP);
        AtomicReference<MultiEmitter<? super TestOutput>> emitterRef = new AtomicReference<>();

        @SuppressWarnings("unchecked")
        Multi<TestOutput> result = (Multi<TestOutput>) runner.publish(
            Multi.createFrom().<TestOutput>emitter(emitter -> emitterRef.set(emitter)));
        AssertSubscriber<TestOutput> subscriber = result.subscribe().withSubscriber(AssertSubscriber.create(1));

        emitterRef.get().emit(new TestOutput("a", "one"));

        subscriber.awaitItems(1, Duration.ofSeconds(5));
        subscriber.assertItems(new TestOutput("a", "one"));
        assertEquals("one\n", provider.sessions.get("a.out").body());

        emitterRef.get().complete();
        subscriber.request(1);
        subscriber.awaitCompletion(Duration.ofSeconds(5));
    }

    @Test
    void publishMultiEnforcesMaxOpenGroups() {
        StreamingRecordingProvider provider = new StreamingRecordingProvider();
        ObjectPublishRunner runner = new ObjectPublishRunner(
            config(target(false, 1), StreamingTestMapper.class.getName()),
            new ObjectTargetRegistry(List.of(provider)),
            ObjectPublishTelemetry.NOOP);

        @SuppressWarnings("unchecked")
        Multi<TestOutput> result = (Multi<TestOutput>) runner.publish(Multi.createFrom().items(
            new TestOutput("a", "one"),
            new TestOutput("b", "two")));

        assertThrows(IllegalStateException.class, () -> result.collect().asList().await().indefinitely());
    }

    @Test
    void objectPublishProviderSpiDoesNotExposeMutinyOrQuarkusTypes() throws Exception {
        List<Class<?>> spiTypes = List.of(
            ObjectTargetProvider.class,
            ObjectWriteSession.class,
            StreamingObjectPublishMapper.class,
            ObjectPublishGroupRenderer.class);

        for (Class<?> spiType : spiTypes) {
            assertFalse(Arrays.stream(spiType.getMethods())
                .flatMap(method -> java.util.stream.Stream.concat(
                    java.util.stream.Stream.of(method.getReturnType()),
                    Arrays.stream(method.getParameterTypes())))
                .map(Class::getName)
                .anyMatch(name -> name.startsWith("io.smallrye.mutiny") || name.startsWith("io.quarkus")),
                spiType.getName() + " must not expose Mutiny or Quarkus types");
        }
        assertEquals(CompletionStage.class,
            ObjectTargetProvider.class.getMethod("write", ObjectWriteRequest.class).getReturnType());
        assertEquals(CompletionStage.class,
            ObjectTargetProvider.class.getMethod("open", ObjectWriteOpenRequest.class).getReturnType());
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
        return target(failing, 32);
    }

    private PipelineObjectPublishConfig target(boolean failing, int maxOpenGroups) {
        return new PipelineObjectPublishConfig(
            "results",
            "object",
            failing ? "failing" : "test",
            Map.of(),
            new PipelineObjectNamingConfig("{groupKey}.out"),
            null,
            new PipelineObjectPublishGroupingConfig(maxOpenGroups));
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

    public static final class StreamingTestMapper implements StreamingObjectPublishMapper<TestOutput> {
        @Override
        public String groupKey(TestOutput item) {
            return item.group();
        }

        @Override
        public ObjectPublishGroupRenderer<TestOutput> openGroup(String groupKey, TestOutput firstItem) {
            return new ObjectPublishGroupRenderer<>() {
                private int count;

                @Override
                public String contentType() {
                    return "text/plain";
                }

                @Override
                public ObjectPayloadChunk onItem(TestOutput item) {
                    count++;
                    return new ObjectPayloadChunk((item.value() + "\n").getBytes(StandardCharsets.UTF_8));
                }

                @Override
                public Map<String, String> finalMetadata() {
                    return Map.of("recordCount", String.valueOf(count));
                }
            };
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
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            writes.add(request);
            return CompletableFuture.completedFuture(
                new ObjectWriteResult(null, request.bytes().length, request.checksum(), null));
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new NoopSession());
        }
    }

    private static final class FailingProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return "failing";
        }

        @Override
        public CompletionStage<ObjectWriteResult> write(ObjectWriteRequest request) {
            return CompletableFuture.failedFuture(new IllegalStateException("publish failed"));
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.failedFuture(new IllegalStateException("publish failed"));
        }
    }

    private static final class DuplicateProvider implements ObjectTargetProvider {
        @Override
        public String providerName() {
            return " TEST ";
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new NoopSession());
        }
    }

    private static final class StreamingRecordingProvider implements ObjectTargetProvider {
        private final Map<String, StreamingRecordingSession> sessions = new LinkedHashMap<>();

        @Override
        public String providerName() {
            return "test";
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            StreamingRecordingSession session = new StreamingRecordingSession(request);
            sessions.put(request.objectKey(), session);
            return CompletableFuture.completedFuture(session);
        }
    }

    private static final class StreamingRecordingSession implements ObjectWriteSession {
        private final ObjectWriteOpenRequest openRequest;
        private final List<byte[]> chunks = new ArrayList<>();
        private Map<String, String> closeMetadata = Map.of();

        private StreamingRecordingSession(ObjectWriteOpenRequest openRequest) {
            this.openRequest = openRequest;
        }

        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            ByteBuffer duplicate = chunk.slice();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            chunks.add(bytes);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest request) {
            closeMetadata = request.metadata();
            return CompletableFuture.completedFuture(new ObjectWriteResult(
                null,
                request.bytes(),
                request.checksum(),
                null));
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.completedFuture(null);
        }

        private String body() {
            return chunks.stream()
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8))
                .reduce("", String::concat);
        }
    }

    private static final class NoopSession implements ObjectWriteSession {
        @Override
        public CompletionStage<Void> write(ByteBuffer chunk) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest request) {
            return CompletableFuture.completedFuture(new ObjectWriteResult(null, request.bytes(), request.checksum(), null));
        }

        @Override
        public CompletionStage<Void> abort(Throwable cause) {
            return CompletableFuture.completedFuture(null);
        }
    }
}
