package org.pipelineframework;

import java.lang.reflect.Field;
import org.pipelineframework.objectpublish.ObjectPublishRunner;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import org.pipelineframework.branching.PipelineBranchingRegistry;
import org.pipelineframework.config.ParallelismPolicy;
import org.pipelineframework.config.PipelineConfig;
import org.pipelineframework.step.ConfigFactory;
import org.pipelineframework.telemetry.PipelineRunTelemetry;
import org.pipelineframework.telemetry.PipelineStepTelemetry;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Runtime test fixture for exercising generated invocation beans without starting Quarkus. */
public final class PipelineRunnerTestHarness {
    private PipelineRunnerTestHarness() {
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
        PipelineRunTelemetry runTelemetry = mock(PipelineRunTelemetry.class);
        PipelineStepTelemetry.Seam stepTelemetry = mock(PipelineStepTelemetry.Seam.class);
        PipelineStepOrderer orderer = mock(PipelineStepOrderer.class);
        runner.runTelemetry = runTelemetry;
        runner.stepTelemetry = stepTelemetry;
        runner.stepOrderer = orderer;
        runner.parallelismPolicyResolver = mock(PipelineParallelismPolicyResolver.class);
        runner.cacheSupportFactory = mock(PipelineCacheSupportFactory.class);
        runner.stepExecutor = new PipelineStepExecutor();
        runner.stepExecutor.branchingRegistry = new PipelineBranchingRegistry();
        when(runner.parallelismPolicyResolver.resolveParallelismPolicy(any())).thenReturn(ParallelismPolicy.SEQUENTIAL);
        when(runner.parallelismPolicyResolver.resolveMaxConcurrency(any())).thenReturn(1);
        when(runner.cacheSupportFactory.buildCacheReadSupport()).thenReturn(null);
        when(orderer.orderSteps(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(runTelemetry.startRun(any(), anyInt(), any(), anyInt()))
            .thenReturn(PipelineRunTelemetry.nonOwningContext());
        when(runTelemetry.instrumentInput(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(runTelemetry.instrumentRunCompletion(any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(stepTelemetry.instrumentItemConsumed(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(stepTelemetry.instrumentItemConsumed(any(), any(), any(Uni.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(stepTelemetry.instrumentItemProduced(any(), any(), any(Multi.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(stepTelemetry.instrumentItemProduced(any(), any(), any(Uni.class)))
            .thenAnswer(invocation -> invocation.getArgument(2));
        lenient().when(stepTelemetry.instrumentStepUni(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(stepTelemetry.instrumentStepUni(any(), any(), any(), anyBoolean()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(stepTelemetry.instrumentStepMulti(any(), any(), any(), anyBoolean(), any()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        lenient().when(stepTelemetry.instrumentStepMulti(any(), any(), any(), anyBoolean()))
            .thenAnswer(invocation -> invocation.getArgument(1));
        ObjectPublishRunner publisher = mock(ObjectPublishRunner.class);
        when(publisher.enabled()).thenReturn(true);
        when(publisher.publish(any())).thenAnswer(invocation -> invocation.getArgument(0));
        setObjectPublishRunner(runner, publisher);
        return new Harness(runner, runTelemetry, orderer, publisher);
    }

    public static void setObjectPublishRunner(PipelineRunner runner, ObjectPublishRunner publishRunner) {
        try {
            Field publisherField = PipelineRunner.class.getDeclaredField("objectPublishRunner");
            publisherField.setAccessible(true);
            publisherField.set(runner, publishRunner);
        } catch (ReflectiveOperationException exception) {
            throw new IllegalStateException("Could not configure PipelineRunner terminal publisher", exception);
        }
    }

    public record Harness(
        PipelineRunner runner,
        PipelineRunTelemetry runTelemetry,
        PipelineStepOrderer orderer,
        ObjectPublishRunner publisher
    ) {
        public Harness maxRecursiveDepth(int maximumDepth) {
            runner.pipelineConfig.maxRecursiveDepth(maximumDepth);
            return this;
        }

        public void verifyRootOrderAppliedOnce() {
            verify(orderer, times(1)).orderSteps(any());
        }
    }
}
