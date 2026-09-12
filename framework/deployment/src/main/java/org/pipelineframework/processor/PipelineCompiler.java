package org.pipelineframework.processor;

import java.util.List;
import java.util.Objects;

/** Executes the compiler's semantic and generation phases in their configured order. */
public final class PipelineCompiler {

    private final List<PipelineCompilationPhase> phases;

    /**
     * Creates a phase engine with an immutable snapshot of the requested ordering.
     *
     * @param phases compilation phases in execution order
     */
    public PipelineCompiler(List<PipelineCompilationPhase> phases) {
        this.phases = List.copyOf(Objects.requireNonNull(phases, "phases must not be null"));
    }

    /**
     * Executes every configured phase once for the supplied compilation context.
     *
     * @param context host-created compilation context
     * @throws PipelineCompilationException when a phase fails
     */
    public void compile(PipelineCompilationContext context) {
        Objects.requireNonNull(context, "context must not be null");
        for (PipelineCompilationPhase phase : phases) {
            try {
                phase.execute(context);
            } catch (Exception cause) {
                throw new PipelineCompilationException(phase.name(), cause);
            }
        }
    }
}
