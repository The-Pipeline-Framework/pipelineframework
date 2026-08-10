package org.pipelineframework.orchestrator.stream;

import java.util.List;
import java.util.Objects;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.UUID;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;

/**
 * One bounded producer-owned page to be atomically made visible with its scalar await work.
 */
public record StreamRegionPageCommit(
    StreamRegionRecord claimedRegion,
    OpaqueSourceCheckpoint nextCheckpoint,
    boolean endOfSource,
    List<AwaitInteractionRecord> interactions,
    long nowEpochMs
) {
    public StreamRegionPageCommit {
        claimedRegion = Objects.requireNonNull(claimedRegion, "claimedRegion must not be null");
        nextCheckpoint = Objects.requireNonNull(nextCheckpoint, "nextCheckpoint must not be null");
        interactions = interactions == null ? List.of() : List.copyOf(interactions);
        if (claimedRegion.status() != StreamRegionStatus.ACTIVE
            || claimedRegion.leaseOwner().isBlank()
            || claimedRegion.leaseExpiresEpochMs() <= nowEpochMs) {
            throw new IllegalArgumentException("stream page requires a current active region claim");
        }
        if (interactions.size() > claimedRegion.availableCredits()) {
            throw new IllegalArgumentException("stream page exceeds available credits");
        }
        if (interactions.isEmpty() && !endOfSource) {
            throw new IllegalArgumentException("zero-item stream page must seal the source to avoid livelock");
        }
        HashSet<String> interactionIds = new HashSet<>();
        for (AwaitInteractionRecord interaction : interactions) {
            int expectedOrdinal = Math.toIntExact(claimedRegion.nextLogicalOrdinal() + interactionIds.size());
            if (!claimedRegion.tenantId().equals(interaction.tenantId())
                || !claimedRegion.executionId().equals(interaction.executionId())
                || !claimedRegion.regionId().equals(interaction.streamRegionId())
                || interaction.itemIndex() == null
                || interaction.itemIndex() != expectedOrdinal
                || !interaction.interactionId().equals(interactionId(claimedRegion, expectedOrdinal))
                || !interactionIds.add(interaction.interactionId())) {
                throw new IllegalArgumentException("stream page interaction must be linked to its claimed region");
            }
        }
    }

    /** Stable per-region ordinal identity used by page retries and ambiguous-commit recovery. */
    public static String interactionId(StreamRegionRecord region, int ordinal) {
        String identity = region.tenantId() + "\n" + region.executionId() + "\n" + region.regionId() + "\n" + ordinal;
        return UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
    }
}
