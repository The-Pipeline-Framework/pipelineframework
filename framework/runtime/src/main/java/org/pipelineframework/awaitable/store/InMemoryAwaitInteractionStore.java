package org.pipelineframework.awaitable.store;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitCreateResult;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;

/**
 * In-memory await store intended for local development and tests.
 */
@ApplicationScoped
public class InMemoryAwaitInteractionStore implements AwaitInteractionStore {

    private static final Comparator<AwaitInteractionRecord> PENDING_ORDER =
        Comparator.comparingLong(AwaitInteractionRecord::deadlineEpochMs)
            .thenComparingLong(AwaitInteractionRecord::createdAtEpochMs)
            .thenComparing(AwaitInteractionRecord::interactionId);

    private final Object lock = new Object();
    private final Map<String, AwaitInteractionRecord> interactionsByScopedId = new HashMap<>();
    private final Map<String, String> interactionIdByScopedIdempotencyKey = new HashMap<>();
    private final Map<String, String> interactionIdByScopedCorrelation = new HashMap<>();

    @Override
    public String providerName() {
        return "memory";
    }

    @Override
    public int priority() {
        return -100;
    }

    @Override
    public Uni<AwaitCreateResult> createOrGet(AwaitCreateCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                String scopedKey = scopedIdempotencyKey(command.tenantId(), command.stepId(), command.idempotencyKey());
                String existingId = interactionIdByScopedIdempotencyKey.get(scopedKey);
                if (existingId != null) {
                    AwaitInteractionRecord existing = interactionsByScopedId.get(scopedInteractionId(command.tenantId(), existingId));
                    if (existing != null && !existing.status().terminal()) {
                        return new AwaitCreateResult(existing, true);
                    }
                }
                String interactionId = UUID.randomUUID().toString();
                AwaitInteractionRecord created = new AwaitInteractionRecord(
                    command.tenantId(),
                    command.executionId(),
                    command.stepId(),
                    command.stepIndex(),
                    command.outputType(),
                    interactionId,
                    command.correlationId(),
                    command.causationId(),
                    command.idempotencyKey(),
                    0L,
                    AwaitInteractionStatus.WAITING,
                    command.requestPayload(),
                    null,
                    command.barrierId(),
                    command.barrierItemIndex(),
                    command.barrierItemCount(),
                    null,
                    command.assignee(),
                    command.group(),
                    command.transportType(),
                    Map.of(),
                    command.deadlineEpochMs(),
                    command.nowEpochMs(),
                    command.nowEpochMs(),
                    command.ttlEpochS());
                interactionsByScopedId.put(scopedInteractionId(created.tenantId(), created.interactionId()), created);
                interactionIdByScopedIdempotencyKey.put(scopedKey, interactionId);
                interactionIdByScopedCorrelation.put(scopedCorrelation(command.tenantId(), command.correlationId()), interactionId);
                return new AwaitCreateResult(created, false);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> get(String tenantId, String interactionId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> findByCorrelation(String tenantId, String correlationId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(tenantId, correlationId));
                return interactionId == null
                    ? Optional.empty()
                    : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(tenantId, interactionId)));
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findByBarrier(
        String tenantId,
        String executionId,
        int stepIndex,
        String barrierId) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                return interactionsByScopedId.values().stream()
                    .filter(record -> Objects.equals(record.tenantId(), tenantId))
                    .filter(record -> Objects.equals(record.executionId(), executionId))
                    .filter(record -> record.stepIndex() == stepIndex)
                    .filter(record -> Objects.equals(record.barrierId(), barrierId))
                    .sorted(Comparator
                        .comparingInt((AwaitInteractionRecord record) -> record.barrierItemIndex() == null
                            ? Integer.MAX_VALUE
                            : record.barrierItemIndex())
                        .thenComparing(record -> nullToEmpty(record.causationId()))
                        .thenComparing(AwaitInteractionRecord::interactionId))
                    .toList();
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatching(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.WAITING,
            current -> updateStatus(current, AwaitInteractionStatus.DISPATCHING, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markDispatched(
        String tenantId,
        String interactionId,
        long expectedVersion,
        Map<String, Object> transportMetadata,
        long nowEpochMs) {
        Map<String, Object> safeMetadata = transportMetadata == null ? Map.of() : Map.copyOf(transportMetadata);
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs,
            AwaitInteractionStatus.DISPATCHING,
            current -> new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            AwaitInteractionStatus.DISPATCHED,
            current.requestPayload(),
            current.responsePayload(),
            current.barrierId(),
            current.barrierItemIndex(),
            current.barrierItemCount(),
            current.actor(),
            current.assignee(),
            current.group(),
            current.transportType(),
            safeMetadata,
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS()));
    }

    @Override
    public Uni<AwaitCompletionResult> complete(AwaitCompletionCommand command) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(command.nowEpochMs());
                AwaitInteractionRecord current = resolveForCompletion(command)
                    .orElseThrow(() -> new IllegalArgumentException("No await interaction matches completion"));
                if (current.status() == AwaitInteractionStatus.COMPLETED) {
                    return new AwaitCompletionResult(current, true);
                }
                if (current.status().terminal()) {
                    throw new IllegalStateException("Await interaction is terminal: " + current.status());
                }
                if (current.deadlineEpochMs() <= command.nowEpochMs()) {
                    AwaitInteractionRecord timedOut = updateStatus(current, AwaitInteractionStatus.TIMED_OUT, command.nowEpochMs(), null, null);
                    interactionsByScopedId.put(scopedInteractionId(timedOut.tenantId(), timedOut.interactionId()), timedOut);
                    throw new IllegalStateException("Await interaction timed out before completion");
                }
                AwaitInteractionRecord completed = new AwaitInteractionRecord(
                    current.tenantId(),
                    current.executionId(),
                    current.stepId(),
                    current.stepIndex(),
                    current.outputType(),
                    current.interactionId(),
                    current.correlationId(),
                    current.causationId(),
                    current.idempotencyKey(),
                    current.version() + 1,
                    AwaitInteractionStatus.COMPLETED,
                    current.requestPayload(),
                    command.responsePayload(),
                    current.barrierId(),
                    current.barrierItemIndex(),
                    current.barrierItemCount(),
                    command.actor(),
                    current.assignee(),
                    current.group(),
                    current.transportType(),
                    current.transportMetadata(),
                    current.deadlineEpochMs(),
                    current.createdAtEpochMs(),
                    command.nowEpochMs(),
                    current.ttlEpochS());
                interactionsByScopedId.put(scopedInteractionId(completed.tenantId(), completed.interactionId()), completed);
                return new AwaitCompletionResult(completed, false);
            }
        });
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> fail(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.FAILED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> cancel(
        String tenantId,
        String interactionId,
        long expectedVersion,
        String reason,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.CANCELLED, nowEpochMs, null, null));
    }

    @Override
    public Uni<Optional<AwaitInteractionRecord>> markTimedOut(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs) {
        return transition(tenantId, interactionId, expectedVersion, nowEpochMs, null,
            current -> updateStatus(current, AwaitInteractionStatus.TIMED_OUT, nowEpochMs, null, null));
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> findTimedOut(long nowEpochMs, int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                List<AwaitInteractionRecord> records = interactionsByScopedId.values().stream()
                    .filter(record -> !record.status().terminal())
                    .filter(record -> record.deadlineEpochMs() <= nowEpochMs)
                    .sorted(PENDING_ORDER)
                    .limit(Math.max(0, limit))
                    .toList();
                return List.copyOf(records);
            }
        });
    }

    @Override
    public Uni<List<AwaitInteractionRecord>> queryPending(
        String tenantId,
        String assignee,
        String group,
        String stepId,
        int limit) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                long now = System.currentTimeMillis();
                purgeExpired(now);
                List<AwaitInteractionRecord> records = new ArrayList<>();
                for (AwaitInteractionRecord record : interactionsByScopedId.values()) {
                    if (record.status().terminal() || !Objects.equals(record.tenantId(), tenantId)) {
                        continue;
                    }
                    if (assignee != null && !Objects.equals(assignee, record.assignee())) {
                        continue;
                    }
                    if (group != null && !Objects.equals(group, record.group())) {
                        continue;
                    }
                    if (stepId != null && !Objects.equals(stepId, record.stepId())) {
                        continue;
                    }
                    records.add(record);
                }
                records.sort(PENDING_ORDER);
                return List.copyOf(records.subList(0, Math.min(records.size(), Math.max(0, limit))));
            }
        });
    }

    private Uni<Optional<AwaitInteractionRecord>> transition(
        String tenantId,
        String interactionId,
        long expectedVersion,
        long nowEpochMs,
        AwaitInteractionStatus requiredStatus,
        java.util.function.Function<AwaitInteractionRecord, AwaitInteractionRecord> transition) {
        return Uni.createFrom().item(() -> {
            synchronized (lock) {
                purgeExpired(nowEpochMs);
                String scopedId = scopedInteractionId(tenantId, interactionId);
                AwaitInteractionRecord current = interactionsByScopedId.get(scopedId);
                if (current == null || current.version() != expectedVersion || current.status().terminal()) {
                    return Optional.empty();
                }
                if (requiredStatus != null && current.status() != requiredStatus) {
                    return Optional.empty();
                }
                AwaitInteractionRecord updated = transition.apply(current);
                interactionsByScopedId.put(scopedId, updated);
                return Optional.of(updated);
            }
        });
    }

    private Optional<AwaitInteractionRecord> resolveForCompletion(AwaitCompletionCommand command) {
        if (command.interactionId() != null && !command.interactionId().isBlank()) {
            return Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), command.interactionId())));
        }
        String interactionId = interactionIdByScopedCorrelation.get(scopedCorrelation(command.tenantId(), command.correlationId()));
        return interactionId == null
            ? Optional.empty()
            : Optional.ofNullable(interactionsByScopedId.get(scopedInteractionId(command.tenantId(), interactionId)));
    }

    private AwaitInteractionRecord updateStatus(
        AwaitInteractionRecord current,
        AwaitInteractionStatus status,
        long nowEpochMs,
        Object responsePayload,
        String actor) {
        return new AwaitInteractionRecord(
            current.tenantId(),
            current.executionId(),
            current.stepId(),
            current.stepIndex(),
            current.outputType(),
            current.interactionId(),
            current.correlationId(),
            current.causationId(),
            current.idempotencyKey(),
            current.version() + 1,
            status,
            current.requestPayload(),
            responsePayload == null ? current.responsePayload() : responsePayload,
            current.barrierId(),
            current.barrierItemIndex(),
            current.barrierItemCount(),
            actor == null ? current.actor() : actor,
            current.assignee(),
            current.group(),
            current.transportType(),
            current.transportMetadata(),
            current.deadlineEpochMs(),
            current.createdAtEpochMs(),
            nowEpochMs,
            current.ttlEpochS());
    }

    private void purgeExpired(long nowEpochMs) {
        long nowEpochS = Instant.ofEpochMilli(nowEpochMs).getEpochSecond();
        var iterator = interactionsByScopedId.entrySet().iterator();
        while (iterator.hasNext()) {
            AwaitInteractionRecord record = iterator.next().getValue();
            if (record.ttlEpochS() > 0 && record.ttlEpochS() <= nowEpochS) {
                iterator.remove();
                interactionIdByScopedIdempotencyKey.remove(scopedIdempotencyKey(
                    record.tenantId(), record.stepId(), record.idempotencyKey()));
                interactionIdByScopedCorrelation.remove(scopedCorrelation(record.tenantId(), record.correlationId()));
            }
        }
    }

    private static String scopedInteractionId(String tenantId, String interactionId) {
        return compositeScopedKey("tenantId", tenantId, "interactionId", interactionId);
    }

    private static String scopedIdempotencyKey(String tenantId, String stepId, String idempotencyKey) {
        return compositeScopedKey("tenantStep", tenantId + ":" + stepId, "idempotencyKey", idempotencyKey);
    }

    private static String scopedCorrelation(String tenantId, String correlationId) {
        return compositeScopedKey("tenantId", tenantId, "correlationId", correlationId);
    }

    private static String compositeScopedKey(String leftName, String left, String rightName, String right) {
        String safeLeft = Objects.requireNonNull(left, leftName + " must not be null");
        String safeRight = Objects.requireNonNull(right, rightName + " must not be null");
        return safeLeft.length() + ":" + safeLeft + ":" + safeRight.length() + ":" + safeRight;
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
