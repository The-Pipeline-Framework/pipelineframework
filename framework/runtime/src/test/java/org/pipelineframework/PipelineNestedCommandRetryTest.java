package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitContinuationMode;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.awaitable.TerminalOutputOwnership;
import org.pipelineframework.command.CommandStep;
import org.pipelineframework.config.StepConfig;
import org.pipelineframework.execution.PipelineExecutionContext;
import org.pipelineframework.execution.PipelineExecutionContextHolder;
import org.pipelineframework.invocation.PipelineInvocationSteps;
import org.pipelineframework.step.ConfigurableStep;
import org.pipelineframework.step.StepOneToOne;

@QuarkusTest
class PipelineNestedCommandRetryTest {

    @Inject
    PipelineRunner runner;

    @AfterEach
    void clearContext() {
        AwaitExecutionContextHolder.clear();
        PipelineExecutionContextHolder.clear();
    }

    @Test
    void rootRetryTargetFlowsThroughNestedDefinitionToCommand() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant",
            "execution",
            0,
            AwaitContinuationMode.DURABLE_HANDOFF,
            TerminalOutputOwnership.COORDINATOR,
            Map.of()));
        PipelineExecutionContextHolder.set(PipelineExecutionContext.forCommandRetry(
            "tenant", "execution", 0, 0, "command-retry:execution:2"));
        AtomicInteger commandCalls = new AtomicInteger();
        StepOneToOne<String, String> nested = PipelineInvocationSteps.oneToOne(
            runner,
            "child-definition",
            1,
            List.of(new PrefixStep(), new RetryClaimingCommandStep(commandCalls)));

        @SuppressWarnings("unchecked")
        Multi<String> result = (Multi<String>) runner.run(Multi.createFrom().item("invoice"), List.of(nested));

        assertEquals("invoice:prepared:command", result.collect().first().await().atMost(Duration.ofSeconds(5)));
        assertEquals(1, commandCalls.get());
    }

    @Test
    void nestedFailureRetainsResumableRootStepIndex() {
        AwaitExecutionContextHolder.set(new AwaitExecutionContext(
            "tenant",
            "execution",
            0,
            AwaitContinuationMode.DURABLE_HANDOFF,
            TerminalOutputOwnership.COORDINATOR,
            Map.of()));
        StepOneToOne<String, String> nested = PipelineInvocationSteps.oneToOne(
            runner,
            "child-definition",
            1,
            List.of(new PrefixStep(), new FailingCommandStep()));

        @SuppressWarnings("unchecked")
        Multi<String> result = (Multi<String>) runner.run(Multi.createFrom().item("invoice"), List.of(nested));
        Throwable failure = org.junit.jupiter.api.Assertions.assertThrows(
            Throwable.class,
            () -> result.collect().first().await().atMost(Duration.ofSeconds(15)));

        assertEquals(0, PipelineStepExecutionFailure.stepIndex(failure), failureChain(failure));
    }

    private static final class PrefixStep extends ConfigurableStep implements StepOneToOne<String, String> {
        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().item(input + ":prepared");
        }
    }

    private static final class RetryClaimingCommandStep extends ConfigurableStep
        implements StepOneToOne<String, String>, CommandStep {
        private final AtomicInteger calls;
        private final StepConfig noRetry = new StepConfig().retryLimit(0);

        private RetryClaimingCommandStep(AtomicInteger calls) {
            this.calls = calls;
        }

        @Override
        public StepConfig effectiveConfig() {
            return noRetry;
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            PipelineExecutionContext context = PipelineExecutionContextHolder.get().orElseThrow();
            assertEquals(0, context.currentStepIndex());
            assertTrue(context.commandRetryTargetsCurrentStep());
            assertTrue(context.claimCommandRetry("archive:invoice"));
            calls.incrementAndGet();
            return Uni.createFrom().item(input + ":command");
        }
    }

    private static final class FailingCommandStep extends ConfigurableStep
        implements StepOneToOne<String, String>, CommandStep {
        private final StepConfig noRetry = new StepConfig().retryLimit(0);

        @Override
        public StepConfig effectiveConfig() {
            return noRetry;
        }

        @Override
        public void initialiseWithConfig(StepConfig ignored) {
            // This fixture must expose the first failure without step-level retry delay.
        }

        @Override
        public Uni<String> applyOneToOne(String input) {
            return Uni.createFrom().failure(new IllegalStateException("command failed"));
        }
    }

    private static String failureChain(Throwable failure) {
        StringBuilder chain = new StringBuilder();
        Throwable current = failure;
        while (current != null) {
            if (!chain.isEmpty()) {
                chain.append(" -> ");
            }
            chain.append(current.getClass().getName());
            current = current.getCause() == current ? null : current.getCause();
        }
        return chain.toString();
    }
}
