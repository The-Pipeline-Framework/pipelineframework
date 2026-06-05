package org.pipelineframework.orchestrator;

import java.util.List;
import java.util.Objects;

/**
 * Portable result returned across the transition-worker seam.
 *
 * @param outcome transition outcome
 * @param outputPayloads serialized output payloads
 * @param awaitSuspension await suspension metadata when waiting externally
 * @param failure failure metadata when failed
 */
public record TransitionResultEnvelope(
    TransitionWorkerOutcome outcome,
    List<SerializedTransitionPayload> outputPayloads,
    TransitionAwaitSuspension awaitSuspension,
    TransitionFailureEnvelope failure) {
    public TransitionResultEnvelope {
        Objects.requireNonNull(outcome, "TransitionResultEnvelope.outcome must not be null");
        outputPayloads = outputPayloads == null ? List.of() : List.copyOf(outputPayloads);
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && awaitSuspension == null) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition envelope requires awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && failure == null) {
            throw new IllegalArgumentException("FAILED transition envelope requires failure");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && awaitSuspension != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include awaitSuspension");
        }
    }

    /**
     * Creates a completed envelope by encoding decoded output items.
     *
     * @param codec payload codec
     * @param outputItems decoded output items
     * @return completed envelope
     */
    public static TransitionResultEnvelope completed(TransitionPayloadCodec codec, List<?> outputItems) {
        Objects.requireNonNull(codec, "codec must not be null");
        List<SerializedTransitionPayload> encoded = outputItems == null
            ? List.of()
            : outputItems.stream().map(codec::encode).toList();
        return new TransitionResultEnvelope(TransitionWorkerOutcome.COMPLETED, encoded, null, null);
    }

    /**
     * Creates a waiting envelope.
     *
     * @param awaitSuspension await suspension metadata
     * @return waiting envelope
     */
    public static TransitionResultEnvelope waiting(TransitionAwaitSuspension awaitSuspension) {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.WAITING_EXTERNAL,
            List.of(),
            awaitSuspension,
            null);
    }

    /**
     * Creates a failed envelope.
     *
     * @param failure transition failure
     * @return failed envelope
     */
    public static TransitionResultEnvelope failed(Throwable failure) {
        Objects.requireNonNull(failure, "failure must not be null");
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.FAILED,
            List.of(),
            null,
            TransitionFailureEnvelope.from(failure));
    }

    /**
     * Decodes completed output payloads.
     *
     * @param codec payload codec
     * @return decoded output items
     */
    public List<?> decodeOutputItems(TransitionPayloadCodec codec) {
        Objects.requireNonNull(codec, "codec must not be null");
        return outputPayloads.stream().map(codec::decode).toList();
    }
}
