package org.pipelineframework.orchestrator;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
    TransitionFailureEnvelope failure,
    @JsonIgnore List<?> decodedOutputItems) {
    public TransitionResultEnvelope(
        TransitionWorkerOutcome outcome,
        List<SerializedTransitionPayload> outputPayloads,
        TransitionAwaitSuspension awaitSuspension,
        TransitionFailureEnvelope failure) {
        this(outcome, outputPayloads, awaitSuspension, failure, null);
    }

    public TransitionResultEnvelope {
        Objects.requireNonNull(outcome, "TransitionResultEnvelope.outcome must not be null");
        outputPayloads = outputPayloads == null ? List.of() : List.copyOf(outputPayloads);
        decodedOutputItems = decodedOutputItems == null ? null : List.copyOf(decodedOutputItems);
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && awaitSuspension == null) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition envelope requires awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && failure == null) {
            throw new IllegalArgumentException("FAILED transition envelope requires failure");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && !outputPayloads.isEmpty() && decodedOutputItems != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include both encoded and decoded outputs");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && awaitSuspension != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.COMPLETED && failure != null) {
            throw new IllegalArgumentException("COMPLETED transition envelope must not include failure");
        }
        if (outcome == TransitionWorkerOutcome.WAITING_EXTERNAL && (!outputPayloads.isEmpty() || failure != null || decodedOutputItems != null)) {
            throw new IllegalArgumentException("WAITING_EXTERNAL transition envelope must only include awaitSuspension");
        }
        if (outcome == TransitionWorkerOutcome.FAILED && (!outputPayloads.isEmpty() || awaitSuspension != null || decodedOutputItems != null)) {
            throw new IllegalArgumentException("FAILED transition envelope must only include failure");
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
     * Creates a completed in-process envelope with decoded output items.
     *
     * @param outputItems decoded output items
     * @return completed envelope
     */
    public static TransitionResultEnvelope completedInProcess(List<?> outputItems) {
        return new TransitionResultEnvelope(
            TransitionWorkerOutcome.COMPLETED,
            List.of(),
            null,
            null,
            outputItems == null ? List.of() : outputItems);
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
        if (decodedOutputItems != null) {
            return decodedOutputItems;
        }
        return outputPayloads.stream().map(codec::decode).toList();
    }

    /**
     * Returns output items in the representation the coordinator should persist.
     * In-process workers can carry decoded Java objects. Remote workers return
     * portable payload envelopes, which the coordinator must not eagerly decode
     * when it may not own the application bundle classes.
     *
     * @return decoded local outputs or serialized remote outputs
     */
    public List<?> coordinatorOutputItems() {
        return decodedOutputItems != null ? decodedOutputItems : outputPayloads;
    }
}
