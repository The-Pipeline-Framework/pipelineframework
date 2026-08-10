package org.pipelineframework.orchestrator.stream;

import java.util.Objects;
import java.util.Optional;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.ResumableSourceDescriptor;

/**
 * Bounded execution-owned projection for incremental expansion progress.
 *
 * <p>It intentionally carries no item set, await state, or accumulated output. Individual scalar
 * interactions own their request/outcome; normal segment commits own their continuations.
 */
public record StreamRegionRecord(
    String tenantId,
    String executionId,
    String regionId,
    ResumableSourceDescriptor source,
    OpaqueSourceCheckpoint checkpoint,
    long nextLogicalOrdinal,
    int outstandingCredits,
    int maxOutstandingCredits,
    boolean terminalScalarSuffix,
    StreamRegionStatus status,
    Optional<Long> finalOrdinal,
    long version,
    String leaseOwner,
    long leaseExpiresEpochMs,
    long nextDueEpochMs,
    long createdAtEpochMs,
    long updatedAtEpochMs,
    long ttlEpochS
) {
    /** Compatibility constructor for regions created before compiler terminal eligibility was persisted. */
    public StreamRegionRecord(
        String tenantId, String executionId, String regionId, ResumableSourceDescriptor source,
        OpaqueSourceCheckpoint checkpoint, long nextLogicalOrdinal, int outstandingCredits,
        int maxOutstandingCredits, StreamRegionStatus status, Optional<Long> finalOrdinal, long version,
        String leaseOwner, long leaseExpiresEpochMs, long nextDueEpochMs, long createdAtEpochMs,
        long updatedAtEpochMs, long ttlEpochS
    ) {
        this(tenantId, executionId, regionId, source, checkpoint, nextLogicalOrdinal, outstandingCredits,
            maxOutstandingCredits, false, status, finalOrdinal, version, leaseOwner, leaseExpiresEpochMs,
            nextDueEpochMs, createdAtEpochMs, updatedAtEpochMs, ttlEpochS);
    }
    public StreamRegionRecord {
        tenantId = requireText(tenantId, "tenantId");
        executionId = requireText(executionId, "executionId");
        regionId = requireText(regionId, "regionId");
        source = Objects.requireNonNull(source, "source must not be null");
        checkpoint = Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        if (nextLogicalOrdinal < 0 || outstandingCredits < 0 || maxOutstandingCredits <= 0
            || outstandingCredits > maxOutstandingCredits || version < 0 || leaseExpiresEpochMs < 0
            || nextDueEpochMs < 0 || createdAtEpochMs < 0 || updatedAtEpochMs < 0 || ttlEpochS < 0) {
            throw new IllegalArgumentException("Invalid bounded stream-region projection values");
        }
        status = Objects.requireNonNull(status, "status must not be null");
        finalOrdinal = finalOrdinal == null ? Optional.empty() : finalOrdinal;
        finalOrdinal.ifPresent(value -> {
            if (value < 0 || value > nextLogicalOrdinal) {
                throw new IllegalArgumentException("finalOrdinal must be within materialised logical range");
            }
        });
        if (status == StreamRegionStatus.SOURCE_SEALED && finalOrdinal.isEmpty()) {
            throw new IllegalArgumentException("SOURCE_SEALED stream region requires finalOrdinal");
        }
        if (status.terminal() && finalOrdinal.isEmpty()) {
            throw new IllegalArgumentException("Terminal stream region must be source sealed");
        }
        leaseOwner = leaseOwner == null ? "" : leaseOwner;
    }

    public boolean sourceSealed() {
        return finalOrdinal.isPresent();
    }

    public int availableCredits() {
        return maxOutstandingCredits - outstandingCredits;
    }

    public StreamRegionRecord claimed(String owner, long nowEpochMs, long leaseMs) {
        return updated(status, checkpoint, nextLogicalOrdinal, outstandingCredits, finalOrdinal,
            version + 1, requireText(owner, "leaseOwner"), nowEpochMs + leaseMs, nextDueEpochMs, nowEpochMs);
    }

    public StreamRegionRecord recordPage(
        OpaqueSourceCheckpoint next,
        int itemCount,
        boolean endOfSource,
        long nowEpochMs
    ) {
        if (itemCount < 0 || itemCount > availableCredits()) {
            throw new IllegalArgumentException("Source page exceeds available bounded credits");
        }
        if (sourceSealed()) {
            throw new IllegalStateException("Cannot materialise a page after SourceSealed");
        }
        long nextOrdinal = nextLogicalOrdinal + itemCount;
        Optional<Long> sealedAt = endOfSource ? Optional.of(nextOrdinal) : Optional.empty();
        StreamRegionStatus nextStatus = endOfSource && outstandingCredits + itemCount == 0
            ? StreamRegionStatus.COMPLETED
            : endOfSource ? StreamRegionStatus.SOURCE_SEALED : StreamRegionStatus.ACTIVE;
        return updated(nextStatus, Objects.requireNonNull(next, "next checkpoint must not be null"), nextOrdinal,
            outstandingCredits + itemCount, sealedAt, version + 1, "", 0L,
            nextStatus.terminal() || endOfSource || itemCount == availableCredits() ? Long.MAX_VALUE : nowEpochMs, nowEpochMs);
    }

    public StreamRegionRecord releaseCredit(long nowEpochMs) {
        if (outstandingCredits == 0) {
            throw new IllegalStateException("Cannot release credit below zero");
        }
        StreamRegionStatus nextStatus = sourceSealed() && outstandingCredits == 1
            ? StreamRegionStatus.COMPLETED
            : status;
        return updated(nextStatus, checkpoint, nextLogicalOrdinal, outstandingCredits - 1, finalOrdinal,
            version + 1, "", 0L, nextStatus.terminal() ? Long.MAX_VALUE : nowEpochMs, nowEpochMs);
    }

    private StreamRegionRecord updated(
        StreamRegionStatus nextStatus,
        OpaqueSourceCheckpoint nextCheckpoint,
        long nextOrdinal,
        int nextOutstandingCredits,
        Optional<Long> nextFinalOrdinal,
        long nextVersion,
        String nextLeaseOwner,
        long nextLeaseExpires,
        long nextDue,
        long nowEpochMs
    ) {
        return new StreamRegionRecord(tenantId, executionId, regionId, source, nextCheckpoint, nextOrdinal,
            nextOutstandingCredits, maxOutstandingCredits, terminalScalarSuffix, nextStatus, nextFinalOrdinal, nextVersion,
            nextLeaseOwner, nextLeaseExpires, nextDue, createdAtEpochMs, nowEpochMs, ttlEpochS);
    }

    private static String requireText(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
