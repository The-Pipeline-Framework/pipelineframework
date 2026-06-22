package org.pipelineframework;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.config.boundary.PipelineObjectNamingConfig;
import org.pipelineframework.config.boundary.PipelineObjectOutputConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishConfig;
import org.pipelineframework.config.boundary.PipelineObjectPublishGroupingConfig;
import org.pipelineframework.config.boundary.PipelineOutputBoundaryConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.objectpublish.ObjectPayloadChunk;
import org.pipelineframework.objectpublish.ObjectPublishGroupRenderer;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import org.pipelineframework.objectpublish.ObjectPublishTelemetry;
import org.pipelineframework.objectpublish.ObjectTargetProvider;
import org.pipelineframework.objectpublish.ObjectTargetRegistry;
import org.pipelineframework.objectpublish.ObjectWriteCloseRequest;
import org.pipelineframework.objectpublish.ObjectWriteOpenRequest;
import org.pipelineframework.objectpublish.ObjectWriteResult;
import org.pipelineframework.objectpublish.ObjectWriteSession;
import org.pipelineframework.objectpublish.StreamingObjectPublishMapper;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineRunnerObjectPublishTest {

    @Mock
    private PipelineTelemetry telemetry;

    @Mock
    private PipelineStepOrderer stepOrderer;

    @Mock
    private PipelineParallelismPolicyResolver parallelismPolicyResolver;

    @Mock
    private PipelineCacheSupportFactory cacheSupportFactory;

    private PipelineRunner runner;
    private GatedObjectTargetProvider provider;

    @BeforeEach
    void setUp() throws Exception {
        AwaitExecutionContextHolder.clear();
        runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = new PipelineConfig();
        runner.telemetry = telemetry;
        runner.stepOrderer = stepOrderer;
        runner.parallelismPolicyResolver = parallelismPolicyResolver;
        runner.cacheSupportFactory = cacheSupportFactory;
        runner.stepExecutor = new PipelineStepExecutor();
        provider = new GatedObjectTargetProvider();
        setObjectPublishRunner(runner, new ObjectPublishRunner(
            objectPublishConfig("gated"),
            new ObjectTargetRegistry(List.of(provider)),
            ObjectPublishTelemetry.NOOP));

        lenient().when(stepOrderer.orderSteps(any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(parallelismPolicyResolver.resolveParallelismPolicy(any())).thenReturn(ParallelismPolicy.SEQUENTIAL);
        lenient().when(parallelismPolicyResolver.resolveMaxConcurrency(any())).thenReturn(1);
        lenient().when(cacheSupportFactory.buildCacheReadSupport()).thenReturn(null);
        lenient().when(telemetry.startRun(any(), anyInt(), any(), anyInt()))
            .thenReturn(null);
        lenient().when(telemetry.instrumentInput(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(telemetry.instrumentRunCompletion(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(telemetry.instrumentItemConsumed(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Uni.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentStepUni(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
    }

    @AfterEach
    void clearAwaitContext() {
        AwaitExecutionContextHolder.clear();
    }

    @Test
    void fullPipelineTerminalRunStreamsThroughObjectPublishBeforeResultCollection() throws Exception {
        PipelineRunner.ExecutionResult execution = runner.runFromStepUntilWithContext(
            Multi.createFrom().item(new TestTerminalOutput("file-a", "line-1")),
            List.of(new IdentityStep()),
            0,
            1);

        @SuppressWarnings("unchecked")
        Multi<TestTerminalOutput> stream = (Multi<TestTerminalOutput>) execution.result();
        CompletableFuture<List<TestTerminalOutput>> result = stream.collect().asList().subscribeAsCompletionStage();

        provider.awaitWriteStarted();
        assertFalse(result.isDone(), "Terminal output collection must wait for object publish chunk acceptance");
        assertEquals(1, provider.writeAttempts());
        provider.acceptWrite();

        assertEquals(List.of(new TestTerminalOutput("file-a", "line-1")), result.get(5, TimeUnit.SECONDS));
        assertEquals("line-1\n", provider.body());
    }

    @Test
    void partialPipelineRunDoesNotInvokeTerminalObjectPublish() throws Exception {
        PipelineRunner.ExecutionResult execution = runner.runFromStepUntilWithContext(
            Multi.createFrom().item(new TestTerminalOutput("file-a", "line-1")),
            List.of(new IdentityStep()),
            0,
            0);

        @SuppressWarnings("unchecked")
        Multi<TestTerminalOutput> stream = (Multi<TestTerminalOutput>) execution.result();

        assertEquals(List.of(new TestTerminalOutput("file-a", "line-1")),
            stream.collect().asList().await().indefinitely());
        assertEquals(0, provider.writeAttempts());
    }

    @Test
    void queueAsyncTransitionWithAwaitContextDoesNotInvokeRunnerObjectPublish() throws Exception {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant-1", "exec-1", 0));

        PipelineRunner.ExecutionResult execution = runner.runFromStepUntilWithContext(
            Multi.createFrom().item(new TestTerminalOutput("file-a", "line-1")),
            List.of(new IdentityStep()),
            0,
            1);

        @SuppressWarnings("unchecked")
        Multi<TestTerminalOutput> stream = (Multi<TestTerminalOutput>) execution.result();

        assertEquals(List.of(new TestTerminalOutput("file-a", "line-1")),
            stream.collect().asList().await().indefinitely());
        assertEquals(0, provider.writeAttempts());
    }

    private static PipelineYamlConfig objectPublishConfig(String provider) {
        return new PipelineYamlConfig(
            "org.pipelineframework",
            "GRPC",
            "COMPUTE",
            List.of(),
            Map.of(),
            Map.of(),
            Map.of("results", new PipelineObjectPublishConfig(
                "results",
                "object",
                provider,
                Map.of(),
                new PipelineObjectNamingConfig("{groupKey}.out"),
                null,
                new PipelineObjectPublishGroupingConfig(32))),
            List.of(),
            null,
            new PipelineOutputBoundaryConfig(null, new PipelineObjectOutputConfig(
                "results",
                TestTerminalOutput.class.getName(),
                "TestTerminalOutput",
                StreamingTransitionMapper.class.getName())));
    }

    private static void setObjectPublishRunner(PipelineRunner runner, ObjectPublishRunner publishRunner)
        throws Exception {
        Field field = PipelineRunner.class.getDeclaredField("objectPublishRunner");
        field.setAccessible(true);
        field.set(runner, publishRunner);
    }

    public record TestTerminalOutput(String group, String value) {
    }

    private static final class IdentityStep extends ConfigurableStep
        implements StepOneToOne<TestTerminalOutput, TestTerminalOutput> {
        @Override
        public Uni<TestTerminalOutput> applyOneToOne(TestTerminalOutput input) {
            return Uni.createFrom().item(input);
        }
    }

    public static final class StreamingTransitionMapper implements StreamingObjectPublishMapper<TestTerminalOutput> {
        @Override
        public String groupKey(TestTerminalOutput item) {
            return item.group();
        }

        @Override
        public ObjectPublishGroupRenderer<TestTerminalOutput> openGroup(String groupKey, TestTerminalOutput firstItem) {
            return new ObjectPublishGroupRenderer<>() {
                @Override
                public String contentType() {
                    return "text/plain";
                }

                @Override
                public ObjectPayloadChunk onItem(TestTerminalOutput item) {
                    return new ObjectPayloadChunk((item.value() + "\n").getBytes(
                        java.nio.charset.StandardCharsets.UTF_8));
                }
            };
        }
    }

    private static final class GatedObjectTargetProvider implements ObjectTargetProvider {
        private final CompletableFuture<Void> writeStarted = new CompletableFuture<>();
        private final CompletableFuture<Void> writeAccepted = new CompletableFuture<>();
        private final AtomicInteger writeAttempts = new AtomicInteger();
        private final AtomicReference<String> body = new AtomicReference<>("");

        @Override
        public String providerName() {
            return "gated";
        }

        @Override
        public CompletionStage<ObjectWriteSession> open(ObjectWriteOpenRequest request) {
            return CompletableFuture.completedFuture(new ObjectWriteSession() {
                @Override
                public CompletionStage<Void> write(ByteBuffer chunk) {
                    ByteBuffer duplicate = chunk.slice();
                    byte[] bytes = new byte[duplicate.remaining()];
                    duplicate.get(bytes);
                    body.updateAndGet(current ->
                        current + new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
                    writeAttempts.incrementAndGet();
                    writeStarted.complete(null);
                    return writeAccepted;
                }

                @Override
                public CompletionStage<ObjectWriteResult> close(ObjectWriteCloseRequest closeRequest) {
                    return CompletableFuture.completedFuture(new ObjectWriteResult(
                        null,
                        closeRequest.bytes(),
                        closeRequest.checksum(),
                        null));
                }

                @Override
                public CompletionStage<Void> abort(Throwable cause) {
                    writeAccepted.complete(null);
                    return CompletableFuture.completedFuture(null);
                }
            });
        }

        private void awaitWriteStarted() throws Exception {
            writeStarted.get(5, TimeUnit.SECONDS);
        }

        private void acceptWrite() {
            writeAccepted.complete(null);
        }

        private int writeAttempts() {
            return writeAttempts.get();
        }

        private String body() {
            return body.get();
        }
    }
}
