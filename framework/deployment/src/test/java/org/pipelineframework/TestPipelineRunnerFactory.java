package org.pipelineframework;

import java.lang.reflect.Field;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.branching.PipelineBranchingRegistry;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.telemetry.PipelineTelemetry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Runtime test fixture for exercising generated invocation beans without starting Quarkus. */
public final class TestPipelineRunnerFactory {
    private TestPipelineRunnerFactory() {
    }

    public static PipelineRunner create() {
        return createHarness().runner();
    }

    public static Harness createHarness() {
        try {
            return createHarnessWithRootPublication();
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Could not create PipelineRunner test harness", exception);
        }
    }

    private static Harness createHarnessWithRootPublication() throws ReflectiveOperationException {
        PipelineRunner runner = new PipelineRunner();
        runner.configFactory = new ConfigFactory();
        runner.pipelineConfig = new PipelineConfig();
        PipelineTelemetry telemetry = mock(PipelineTelemetry.class);
        PipelineStepOrderer orderer = mock(PipelineStepOrderer.class);
        runner.telemetry = telemetry;
        runner.stepOrderer = orderer;
        runner.parallelismPolicyResolver = mock(PipelineParallelismPolicyResolver.class);
        runner.cacheSupportFactory = mock(PipelineCacheSupportFactory.class);
        runner.stepExecutor = new PipelineStepExecutor();
        runner.stepExecutor.branchingRegistry = new PipelineBranchingRegistry();
        when(runner.parallelismPolicyResolver.resolveParallelismPolicy(any())).thenReturn(ParallelismPolicy.SEQUENTIAL);
        when(runner.parallelismPolicyResolver.resolveMaxConcurrency(any())).thenReturn(1);
        when(runner.cacheSupportFactory.buildCacheReadSupport()).thenReturn(null);
        when(orderer.orderSteps(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(telemetry.startRun(any(), anyInt(), any(), anyInt()))
            .thenReturn(PipelineTelemetry.RunContext.disabled());
        when(telemetry.instrumentInput(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(telemetry.instrumentRunCompletion(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(telemetry.instrumentItemConsumed(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentItemProduced(any(), any(), any(Uni.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(telemetry.instrumentStepUni(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(telemetry.instrumentStepUni(any(), any(), any(), anyBoolean()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(telemetry.instrumentStepMulti(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(telemetry.instrumentStepMulti(any(), any(), any(), anyBoolean()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        ObjectPublishRunner publisher = mock(ObjectPublishRunner.class);
        when(publisher.enabled()).thenReturn(true);
        when(publisher.publish(any())).thenAnswer(invocation -> invocation.getArgument(0));
        Field publisherField = PipelineRunner.class.getDeclaredField("objectPublishRunner");
        publisherField.setAccessible(true);
        publisherField.set(runner, publisher);
        return new Harness(runner, telemetry, orderer, publisher);
    }

    public record Harness(
        PipelineRunner runner,
        PipelineTelemetry telemetry,
        PipelineStepOrderer orderer,
        ObjectPublishRunner publisher
    ) {
        public void verifyRootOrderAppliedOnce() {
            verify(orderer, times(1)).orderSteps(any());
        }
    }
}
