package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * Portable transition result exchanged by remote runtime transports.
 *
 * @param outcome transition outcome
 * @param outputPayloads serialized output payloads
 * @param awaitSuspension await suspension metadata when waiting externally
 * @param failure failure metadata when failed
 * @param terminalOutputPublished whether the terminal output was already published
 * @param terminalInputPassthrough whether the terminal input remains coordinator-owned
 */
public record TransitionWireResult(
    TransitionWorkerOutcome outcome,
    List<SerializedTransitionPayload> outputPayloads,
    TransitionAwaitSuspension awaitSuspension,
    TransitionFailureEnvelope failure,
    boolean terminalOutputPublished,
    boolean terminalInputPassthrough
) {
    public TransitionWireResult {
        Objects.requireNonNull(outcome, "TransitionWireResult.outcome must not be null");
        outputPayloads = outputPayloads == null ? List.of() : List.copyOf(outputPayloads);
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && awaitSuspension == null) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition result requires awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && failure == null) {
            throw new IllegalArgumentException("FAILED transition result requires failure");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && awaitSuspension != null) {
            throw new IllegalArgumentException("COMPLETED transition result must not include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && failure != null) {
            throw new IllegalArgumentException("COMPLETED transition result must not include failure");
        }
        if (outcome != TransitionWorkerOutcome.COMPLETED && terminalOutputPublished) {
            throw new IllegalArgumentException("Only COMPLETED transition results may mark terminal output as published");
        }
        if (outcome != TransitionWorkerOutcome.COMPLETED && terminalInputPassthrough) {
            throw new IllegalArgumentException("Only COMPLETED transition results may retain terminal input");
        }
        if (terminalOutputPublished && terminalInputPassthrough) {
            throw new IllegalArgumentException("A terminal transition cannot publish output and retain terminal input");
        }
        if (terminalInputPassthrough && !outputPayloads.isEmpty()) {
            throw new IllegalArgumentException(
                "A terminal input passthrough transition must not include output payloads");
        }
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL
            && (!outputPayloads.isEmpty() || failure != null)) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition result must only include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED
            && (!outputPayloads.isEmpty() || awaitSuspension != null)) {
            throw new IllegalArgumentException("FAILED transition result must only include failure");
        }
    }
}
