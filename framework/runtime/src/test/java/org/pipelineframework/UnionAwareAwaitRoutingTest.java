package org.pipelineframework;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.branching.BranchExecutionTracker;
import org.pipelineframework.branching.BranchVariantIdentity;
import org.pipelineframework.branching.StepBranchingDescriptor;
import org.pipelineframework.telemetry.PipelineStepTelemetry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class UnionAwareAwaitRoutingTest {

    @Test
    void acceptedVariantIsUnwrappedBeforeGeneratedAwaitInvocation() {
        ClarificationRequired request = new ClarificationRequired("finding-1");
        Object input = new Decision.Clarify(request);
        AtomicReference<Object> projectorRequest = new AtomicReference<>();
        Prepared projected = new Prepared("finding-1");

        Object result = execution().execute(AwaitLikeStep.class, input, false, (applicable, replay) -> {
            projectorRequest.set(applicable);
            return Uni.createFrom().item(projected);
        }).await().indefinitely();

        assertSame(request, projectorRequest.get());
        assertSame(projected, result);
    }

    @Test
    void nonAcceptedVariantPassesThroughWithoutInvokingAwait() {
        Object ready = new Decision.Ready(new Prepared("ready"));
        AtomicBoolean invoked = new AtomicBoolean();

        Object result = execution().execute(AwaitLikeStep.class, ready, false, (applicable, replay) -> {
            invoked.set(true);
            return Uni.createFrom().item(applicable);
        }).await().indefinitely();

        assertFalse(invoked.get());
        assertSame(ready, result);
    }

    private BranchAwareOneToOneExecution execution() {
        StepBranchingDescriptor descriptor = new StepBranchingDescriptor(
            1,
            "clarify",
            "Clarify",
            ClarificationRequired.class.getName(),
            ClarificationRequired.class,
            List.of(ClarificationRequired.class.getName()),
            List.of(ClarificationRequired.class.getName()),
            List.of(ClarificationRequired.class),
            List.of(
                new BranchVariantIdentity("Decision", "ready", Prepared.class.getName()),
                new BranchVariantIdentity("Decision", "clarify", ClarificationRequired.class.getName())),
            List.of(new BranchVariantIdentity(
                "Decision", "clarify", ClarificationRequired.class.getName())),
            List.of(),
            false);
        return new BranchAwareOneToOneExecution(
            Optional.of(descriptor), new BranchExecutionTracker(), PipelineStepTelemetry.disabled());
    }

    private static final class AwaitLikeStep {
    }

    private sealed interface Decision permits Decision.Ready, Decision.Clarify {
        record Ready(Prepared value) implements Decision {
        }

        record Clarify(ClarificationRequired value) implements Decision {
        }
    }

    private record Prepared(String id) {
    }

    private record ClarificationRequired(String id) {
    }
}
