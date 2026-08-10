package org.pipelineframework.stream;

import org.pipelineframework.step.StepOneToOne;

/**
 * Generated, release-pinned continuation for one resumable expansion producer.
 *
 * <p>The coordinator uses the static await binding only after a page returns. The transition
 * worker runs {@link #transitionStep()} through the normal {@code PipelineRunner}; only the
 * generated step resolves and invokes the provider capability.
 */
public interface StreamRegionContinuation {

    /**
     * Producer cursor that this generated continuation replaces.
     *
     * <p>This is release-pinned compiler metadata. It lets the coordinator select a continuation
     * before invoking the producer, without treating the continuation as a normal ordered step.
     */
    int producerStepIndex();

    /**
     * Whether compiler eligibility proved that every item continuation reaches a terminal scalar
     * suffix. Only this fact permits the final stream credit transaction to complete its parent.
     */
    boolean terminalScalarSuffix();

    ResumableSourceDescriptor descriptor();

    StreamRegionAwaitBinding awaitBinding();

    /** Creates this continuation's concrete, transport-portable input from durable source state. */
    StreamRegionContinuationInput inputFor(
        Object canonicalSourceInput,
        OpaqueSourceCheckpoint checkpoint,
        int limit);

    StepOneToOne<?, ?> transitionStep();
}
