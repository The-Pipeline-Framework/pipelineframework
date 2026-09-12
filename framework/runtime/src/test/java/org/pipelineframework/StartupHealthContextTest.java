package org.pipelineframework;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.telemetry.PipelineRunContext;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StartupHealthContextTest {
    @ParameterizedTest
    @CsvSource({"false,false", "true,false", "false,true", "true,true"})
    void callerExecutionIdentitySurvivesPendingStartupHealth(boolean streaming, boolean runnerFails) throws Exception {
        PipelineExecutionService service = new PipelineExecutionService();
        service.pipelineStepResolver = mock(PipelineStepResolver.class);
        service.pipelineRunner = mock(PipelineRunner.class);
        service.pipelineStepConfig = mock(PipelineStepConfig.class);
        service.executionHooks = new ExecutionHooks();
        service.executionInputPolicy = new ExecutionInputPolicy();
        PipelineStepConfig.HealthConfig health = mock(PipelineStepConfig.HealthConfig.class);
        when(service.pipelineStepConfig.health()).thenReturn(health);
        when(health.startupTimeout()).thenReturn(Duration.ofSeconds(10));
        when(service.pipelineStepResolver.loadPipelineSteps()).thenReturn(List.of(new Object()));
        AtomicReference<Optional<PipelineExecutionContext>> observed = new AtomicReference<>(Optional.empty());
        AtomicReference<Optional<PipelineExecutionContext>> restored = new AtomicReference<>(Optional.empty());
        RuntimeException runnerFailure = new IllegalStateException("runner failed");
        when(service.pipelineRunner.runWithContext(any(), any())).thenAnswer(invocation -> {
            observed.set(PipelineExecutionContextHolder.get());
            if (runnerFails) {
                throw runnerFailure;
            }
            Object result = streaming ? Multi.createFrom().item("done") : Uni.createFrom().item("done");
            return new PipelineRunner.ExecutionResult(result, mock(PipelineRunContext.class));
        });
        CompletableFuture<Boolean> startupHealth = new CompletableFuture<>();
        var field = PipelineExecutionService.class.getDeclaredField("startupHealthFuture");
        field.setAccessible(true);
        field.set(service, startupHealth);
        PipelineExecutionContext context = new PipelineExecutionContext("tenant", "same-execution", 0);
        PipelineExecutionContext workerContext = new PipelineExecutionContext("worker", "unrelated-execution", 0);
        PipelineExecutionContextHolder.set(new PipelineExecutionContext("assembly", "not-subscribed", 0));
        try {
            Uni<String> execution = streaming
                ? service.<String>executePipelineStreaming(Uni.createFrom().item("input"))
                    .collect().asList().map(List::getFirst)
                : service.<String>executePipelineUnary(Uni.createFrom().item("input"));
            PipelineExecutionContextHolder.set(context);
            UniAssertSubscriber<String> subscriber = execution.subscribe().withSubscriber(UniAssertSubscriber.create());
            subscriber.assertNotTerminated();
            Thread healthWorker = Thread.ofPlatform().start(() -> {
                PipelineExecutionContextHolder.set(workerContext);
                try {
                    startupHealth.complete(true);
                    restored.set(PipelineExecutionContextHolder.get());
                } finally {
                    PipelineExecutionContextHolder.clear();
                }
            });
            healthWorker.join();
            if (runnerFails) {
                subscriber.awaitFailure();
                assertSame(runnerFailure, subscriber.getFailure());
            } else {
                subscriber.awaitItem().assertItem("done");
            }
            assertEquals(Optional.of(context), observed.get());
            assertEquals(Optional.of(workerContext), restored.get());
            assertEquals(Optional.of(context), PipelineExecutionContextHolder.get());
        } finally {
            PipelineExecutionContextHolder.clear();
        }
    }
}
