package org.pipelineframework.invocation;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import jakarta.annotation.PreDestroy;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.config.PipelineResilienceConfig;
import org.pipelineframework.runtime.core.resilience.CircuitIdentity;
import org.pipelineframework.runtime.core.resilience.CircuitOutcome;
import org.pipelineframework.runtime.core.resilience.CircuitPolicy;
import org.pipelineframework.runtime.core.resilience.SharedCircuitProbe;
import org.pipelineframework.runtime.core.resilience.SharedCircuitProbeDecision;
import org.pipelineframework.runtime.core.resilience.SharedCircuitSnapshot;
import org.pipelineframework.runtime.core.resilience.SharedCircuitStateStore;
import org.pipelineframework.runtime.core.resilience.SharedCircuitStatus;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * Single-region DynamoDB authority for SHARED_DEPENDENCY circuits.
 *
 * <p>State writes are immutable in-process records replaced with a conditional {@code PutItem};
 * this deliberately avoids update/upsert expressions. Dynamo TTL attributes are cleanup only:
 * every lease condition checks its logical expiry.</p>
 */
final class DynamoSharedCircuitStateStore implements SharedCircuitStateStore {
    private static final String PK = "PK";
    private static final String SK = "SK";
    private static final String STATE_SK = "STATE";
    private static final String POLICY = "policy";
    private static final String STATUS = "status";
    private static final String EPOCH = "epoch";
    private static final String VERSION = "version";
    private static final String OPEN_UNTIL = "open_until_ms";
    private static final String BUCKETS = "failure_buckets";
    private static final String LEASE_TOKEN = "lease_token";
    private static final String LEASE_EXPIRES = "lease_expires_ms";
    private static final String LEASE_OWNER = "lease_owner";
    private static final String TTL = "ttl_epoch_s";
    private static final int BUCKET_COUNT = 60;
    private static final int MAX_CAS_RETRIES = 8;

    private final PipelineResilienceConfig.SharedConfig config;
    private final Clock clock;
    private volatile DynamoDbClient client;

    DynamoSharedCircuitStateStore(PipelineResilienceConfig.SharedConfig config, Clock clock) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
    }

    DynamoSharedCircuitStateStore(DynamoDbClient client, PipelineResilienceConfig.SharedConfig config, Clock clock) {
        this(config, clock);
        this.client = Objects.requireNonNull(client, "client must not be null");
    }

    @Override
    public CompletionStage<SharedCircuitSnapshot> read(CircuitIdentity identity, CircuitPolicy policy) {
        return blocking(() -> readState(identity, policy).snapshot());
    }

    @Override
    public CompletionStage<SharedCircuitSnapshot> recordHealthFailures(
        CircuitIdentity identity,
        CircuitPolicy policy,
        int failures,
        Instant observedAt
    ) {
        if (failures < 1) {
            return read(identity, policy);
        }
        return blocking(() -> recordFailures(identity, policy, failures, observedAt));
    }

    @Override
    public CompletionStage<SharedCircuitProbeDecision> acquireHalfOpenProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        String owner,
        Instant now
    ) {
        return blocking(() -> acquireProbe(identity, policy, owner, now));
    }

    @Override
    public CompletionStage<Void> completeProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        SharedCircuitProbe probe,
        CircuitOutcome outcome,
        Instant completedAt
    ) {
        return blockingVoid(() -> completeProbeBlocking(identity, policy, probe, outcome, completedAt));
    }

    private SharedCircuitSnapshot recordFailures(
        CircuitIdentity identity,
        CircuitPolicy policy,
        int failures,
        Instant observedAt
    ) {
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            StateRecord current = readState(identity, policy);
            if (current.snapshot.status() != SharedCircuitStatus.CLOSED) {
                return current.snapshot;
            }
            Map<Long, Integer> buckets = pruneBuckets(current.buckets, policy, observedAt);
            long bucket = bucketStart(policy, observedAt);
            buckets.merge(bucket, failures, Integer::sum);
            int totalFailures = buckets.values().stream().mapToInt(Integer::intValue).sum();
            SharedCircuitSnapshot updatedSnapshot = totalFailures >= policy.failureThreshold()
                ? new SharedCircuitSnapshot(SharedCircuitStatus.OPEN, current.snapshot.epoch() + 1,
                    current.snapshot.version() + 1, observedAt.plus(policy.openDuration()))
                : new SharedCircuitSnapshot(SharedCircuitStatus.CLOSED, current.snapshot.epoch(),
                    current.snapshot.version() + 1, Instant.MIN);
            StateRecord updated = new StateRecord(updatedSnapshot, fingerprint(policy), buckets);
            if (putState(identity, current, updated)) {
                return updatedSnapshot;
            }
        }
        throw new IllegalStateException("Shared circuit state changed too frequently for " + identity.value());
    }

    private SharedCircuitProbeDecision acquireProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        String owner,
        Instant now
    ) {
        for (int retry = 0; retry < MAX_CAS_RETRIES; retry++) {
            StateRecord current = readState(identity, policy);
            if (current.snapshot.status() == SharedCircuitStatus.CLOSED) {
                return new SharedCircuitProbeDecision(current.snapshot, Optional.empty(), now);
            }
            if (current.snapshot.status() == SharedCircuitStatus.OPEN && now.isBefore(current.snapshot.openUntil())) {
                return new SharedCircuitProbeDecision(current.snapshot, Optional.empty(), current.snapshot.openUntil());
            }
            StateRecord halfOpen = current;
            if (current.snapshot.status() == SharedCircuitStatus.OPEN) {
                SharedCircuitSnapshot snapshot = new SharedCircuitSnapshot(SharedCircuitStatus.HALF_OPEN,
                    current.snapshot.epoch() + 1, current.snapshot.version() + 1, Instant.MIN);
                halfOpen = new StateRecord(snapshot, current.fingerprint, current.buckets);
                Optional<SharedCircuitProbe> first = firstAvailableProbe(identity, policy, owner, now, halfOpen, current);
                if (first.isPresent()) {
                    return new SharedCircuitProbeDecision(halfOpen.snapshot, first, now);
                }
                continue;
            }
            Optional<SharedCircuitProbe> probe = claimExistingHalfOpenSlot(identity, policy, owner, now, halfOpen);
            if (probe.isPresent()) {
                return new SharedCircuitProbeDecision(halfOpen.snapshot, probe, now);
            }
            if (readState(identity, policy).snapshot.version() != halfOpen.snapshot.version()) {
                continue;
            }
            return new SharedCircuitProbeDecision(halfOpen.snapshot, Optional.empty(), earliestProbeExpiry(identity,
                halfOpen.snapshot.epoch(), now, policy));
        }
        throw new IllegalStateException("Shared half-open circuit changed too frequently for " + identity.value());
    }

    private Optional<SharedCircuitProbe> firstAvailableProbe(
        CircuitIdentity identity,
        CircuitPolicy policy,
        String owner,
        Instant now,
        StateRecord halfOpen,
        StateRecord expected
    ) {
        for (int slot = 0; slot < policy.halfOpenMaxPermits(); slot++) {
            SharedCircuitProbe probe = newProbe(halfOpen.snapshot.epoch(), slot, policy, now);
            try {
                dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(List.of(
                    TransactWriteItem.builder().put(Put.builder()
                        .tableName(table()).item(stateItem(identity, halfOpen))
                        .conditionExpression("#version = :expected")
                        .expressionAttributeNames(Map.of("#version", VERSION))
                        .expressionAttributeValues(Map.of(":expected", number(expected.snapshot.version())))
                        .build()).build(),
                    TransactWriteItem.builder().put(probePut(identity, probe, owner, now)).build())).build());
                return Optional.of(probe);
            } catch (TransactionCanceledException ignored) {
                // Either another replica advanced the state or this fixed slot was leased. Try the next slot.
            }
        }
        return Optional.empty();
    }

    private Optional<SharedCircuitProbe> claimExistingHalfOpenSlot(
        CircuitIdentity identity,
        CircuitPolicy policy,
        String owner,
        Instant now,
        StateRecord state
    ) {
        for (int slot = 0; slot < policy.halfOpenMaxPermits(); slot++) {
            SharedCircuitProbe probe = newProbe(state.snapshot.epoch(), slot, policy, now);
            try {
                dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(List.of(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder().tableName(table())
                        .key(stateKey(identity))
                        .conditionExpression("#status = :half AND #epoch = :epoch AND #version = :version")
                        .expressionAttributeNames(Map.of("#status", STATUS, "#epoch", EPOCH, "#version", VERSION))
                        .expressionAttributeValues(Map.of(":half", string(SharedCircuitStatus.HALF_OPEN.name()),
                            ":epoch", number(state.snapshot.epoch()), ":version", number(state.snapshot.version())))
                        .build()).build(),
                    TransactWriteItem.builder().put(probePut(identity, probe, owner, now)).build())).build());
                return Optional.of(probe);
            } catch (TransactionCanceledException ignored) {
                // Another current lease occupies this slot.
            }
        }
        return Optional.empty();
    }

    private void completeProbeBlocking(
        CircuitIdentity identity,
        CircuitPolicy policy,
        SharedCircuitProbe probe,
        CircuitOutcome outcome,
        Instant completedAt
    ) {
        StateRecord current = readState(identity, policy);
        if (current.snapshot.status() != SharedCircuitStatus.HALF_OPEN || current.snapshot.epoch() != probe.epoch()) {
            return;
        }
        if (outcome == CircuitOutcome.NEUTRAL) {
            deleteProbe(identity, probe);
            return;
        }
        SharedCircuitSnapshot next = outcome == CircuitOutcome.SUCCESS
            ? new SharedCircuitSnapshot(SharedCircuitStatus.CLOSED, current.snapshot.epoch() + 1,
                current.snapshot.version() + 1, Instant.MIN)
            : new SharedCircuitSnapshot(SharedCircuitStatus.OPEN, current.snapshot.epoch() + 1,
                current.snapshot.version() + 1, completedAt.plus(policy.openDuration()));
        StateRecord updated = new StateRecord(next, current.fingerprint,
            outcome == CircuitOutcome.SUCCESS ? Map.of() : current.buckets);
        try {
            dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder().transactItems(List.of(
                TransactWriteItem.builder().put(Put.builder().tableName(table()).item(stateItem(identity, updated))
                    .conditionExpression("#version = :version AND #epoch = :epoch AND #status = :half")
                    .expressionAttributeNames(Map.of("#version", VERSION, "#epoch", EPOCH, "#status", STATUS))
                    .expressionAttributeValues(Map.of(":version", number(current.snapshot.version()),
                        ":epoch", number(probe.epoch()), ":half", string(SharedCircuitStatus.HALF_OPEN.name())))
                    .build()).build(),
                TransactWriteItem.builder().delete(Delete.builder().tableName(table()).key(probeKey(identity, probe))
                    .conditionExpression("#token = :token AND #expires >= :now")
                    .expressionAttributeNames(Map.of("#token", LEASE_TOKEN, "#expires", LEASE_EXPIRES))
                    .expressionAttributeValues(Map.of(":token", string(probe.leaseToken()),
                        ":now", number(completedAt.toEpochMilli())))
                    .build()).build())).build());
        } catch (TransactionCanceledException ignored) {
            // A stale epoch, expired lease, or a winning concurrent completion cannot alter newer state.
        }
    }

    private void deleteProbe(CircuitIdentity identity, SharedCircuitProbe probe) {
        try {
            dynamoClient().deleteItem(software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest.builder()
                .tableName(table()).key(probeKey(identity, probe))
                .conditionExpression("#token = :token")
                .expressionAttributeNames(Map.of("#token", LEASE_TOKEN))
                .expressionAttributeValues(Map.of(":token", string(probe.leaseToken())))
                .build());
        } catch (ConditionalCheckFailedException ignored) {
            // Lease expiry/replacement makes a neutral completion stale.
        }
    }

    private Instant earliestProbeExpiry(
        CircuitIdentity identity,
        long epoch,
        Instant now,
        CircuitPolicy policy
    ) {
        Optional<Instant> earliestLeaseExpiry = Optional.empty();
        for (int slot = 0; slot < policy.halfOpenMaxPermits(); slot++) {
            Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder().tableName(table())
                .key(Map.of(PK, string(partitionKey(identity)), SK, string(probeSortKey(epoch, slot))))
                .consistentRead(true).build()).item();
            long expiry = numeric(item.get(LEASE_EXPIRES));
            if (expiry > now.toEpochMilli()) {
                Instant candidate = Instant.ofEpochMilli(expiry);
                if (earliestLeaseExpiry.isEmpty() || candidate.isBefore(earliestLeaseExpiry.orElseThrow())) {
                    earliestLeaseExpiry = Optional.of(candidate);
                }
            }
        }
        Instant retryAfter = now.plus(policy.halfOpenRetryDelay());
        if (earliestLeaseExpiry.isEmpty() || !earliestLeaseExpiry.orElseThrow().isAfter(retryAfter)) {
            return retryAfter;
        }
        return earliestLeaseExpiry.orElseThrow();
    }

    private StateRecord readState(CircuitIdentity identity, CircuitPolicy policy) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder().tableName(table())
            .key(stateKey(identity)).consistentRead(true).build()).item();
        if (item == null || item.isEmpty()) {
            return new StateRecord(SharedCircuitSnapshot.closed(), fingerprint(policy), Map.of());
        }
        StateRecord state = fromStateItem(item);
        if (!state.fingerprint.equals(fingerprint(policy))) {
            throw new IllegalStateException("Shared circuit policy fingerprint mismatch for " + identity.value());
        }
        return state;
    }

    private boolean putState(CircuitIdentity identity, StateRecord expected, StateRecord updated) {
        boolean initial = expected.snapshot.version() == 0 && expected.snapshot.status() == SharedCircuitStatus.CLOSED
            && expected.buckets.isEmpty();
        String condition = initial ? "attribute_not_exists(#pk)" : "#version = :expected";
        Map<String, String> names = initial ? Map.of("#pk", PK) : Map.of("#version", VERSION);
        Map<String, AttributeValue> values = initial ? Map.of() : Map.of(":expected", number(expected.snapshot.version()));
        try {
            dynamoClient().putItem(PutItemRequest.builder().tableName(table()).item(stateItem(identity, updated))
                .conditionExpression(condition).expressionAttributeNames(names).expressionAttributeValues(values).build());
            return true;
        } catch (ConditionalCheckFailedException ignored) {
            return false;
        }
    }

    private static SharedCircuitProbe newProbe(long epoch, int slot, CircuitPolicy policy, Instant now) {
        return new SharedCircuitProbe(epoch, slot, UUID.randomUUID().toString(), now.plus(policy.halfOpenProbeLeaseDuration()));
    }

    private static Map<Long, Integer> pruneBuckets(Map<Long, Integer> input, CircuitPolicy policy, Instant now) {
        long cutoff = now.minus(policy.failureWindow()).toEpochMilli();
        Map<Long, Integer> result = new HashMap<>();
        input.forEach((start, count) -> {
            if (start >= cutoff && count > 0) {
                result.put(start, count);
            }
        });
        return result;
    }

    private static long bucketStart(CircuitPolicy policy, Instant instant) {
        long width = Math.max(1L, policy.failureWindow().toMillis() / BUCKET_COUNT);
        return Math.floorDiv(instant.toEpochMilli(), width) * width;
    }

    private Put probePut(CircuitIdentity identity, SharedCircuitProbe probe, String owner, Instant now) {
        return Put.builder().tableName(table()).item(probeItem(identity, probe, owner))
            .conditionExpression("attribute_not_exists(#pk) OR #expires < :now")
            .expressionAttributeNames(Map.of("#pk", PK, "#expires", LEASE_EXPIRES))
            .expressionAttributeValues(Map.of(":now", number(now.toEpochMilli()))).build();
    }

    private static Map<String, AttributeValue> stateKey(CircuitIdentity identity) {
        return Map.of(PK, string(partitionKey(identity)), SK, string(STATE_SK));
    }

    private static Map<String, AttributeValue> probeKey(CircuitIdentity identity, SharedCircuitProbe probe) {
        return Map.of(PK, string(partitionKey(identity)), SK, string(probeSortKey(probe.epoch(), probe.slot())));
    }

    private static Map<String, AttributeValue> stateItem(CircuitIdentity identity, StateRecord state) {
        Map<String, AttributeValue> item = new HashMap<>(stateKey(identity));
        item.put(POLICY, string(state.fingerprint));
        item.put(STATUS, string(state.snapshot.status().name()));
        item.put(EPOCH, number(state.snapshot.epoch()));
        item.put(VERSION, number(state.snapshot.version()));
        item.put(OPEN_UNTIL, number(openUntilEpochMs(state.snapshot.openUntil())));
        Map<String, AttributeValue> buckets = new HashMap<>();
        state.buckets.forEach((start, count) -> buckets.put(Long.toString(start), number(count)));
        item.put(BUCKETS, AttributeValue.builder().m(buckets).build());
        return Map.copyOf(item);
    }

    private static Map<String, AttributeValue> probeItem(CircuitIdentity identity, SharedCircuitProbe probe, String owner) {
        return Map.of(PK, string(partitionKey(identity)), SK, string(probeSortKey(probe.epoch(), probe.slot())),
            LEASE_TOKEN, string(probe.leaseToken()), LEASE_OWNER, string(owner),
            LEASE_EXPIRES, number(probe.expiresAt().toEpochMilli()),
            TTL, number(Math.max(1L, probe.expiresAt().getEpochSecond())));
    }

    private static StateRecord fromStateItem(Map<String, AttributeValue> item) {
        SharedCircuitSnapshot snapshot = new SharedCircuitSnapshot(
            SharedCircuitStatus.valueOf(text(item.get(STATUS))), numeric(item.get(EPOCH)), numeric(item.get(VERSION)),
            Instant.ofEpochMilli(numeric(item.get(OPEN_UNTIL))));
        Map<Long, Integer> buckets = new HashMap<>();
        Optional.ofNullable(item.get(BUCKETS)).map(AttributeValue::m).orElse(Map.of()).forEach((start, count) ->
            buckets.put(Long.parseLong(start), Math.toIntExact(numeric(count))));
        return new StateRecord(snapshot, text(item.get(POLICY)), Map.copyOf(buckets));
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
                config.region().map(String::trim).filter(value -> !value.isEmpty()).ifPresent(value -> builder.region(Region.of(value)));
                config.endpointOverride().map(String::trim).filter(value -> !value.isEmpty())
                    .ifPresent(value -> builder.endpointOverride(URI.create(value)));
                client = builder.build();
            }
            return client;
        }
    }

    private String table() {
        return config.dynamoTable().map(String::trim).filter(value -> !value.isEmpty())
            .orElseThrow(() -> new IllegalStateException("pipeline.resilience.shared.dynamo-table must not be blank"));
    }

    @PreDestroy
    void close() {
        DynamoDbClient active = client;
        if (active != null) {
            active.close();
        }
    }

    private static <T> CompletionStage<T> blocking(java.util.function.Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribeAsCompletionStage();
    }

    private static CompletionStage<Void> blockingVoid(Runnable runnable) {
        return Uni.createFrom().voidItem().invoke(runnable).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribeAsCompletionStage();
    }

    private static String fingerprint(CircuitPolicy policy) {
        String value = policy.requiredScope() + "|" + policy.failureThreshold() + "|" + policy.failureWindow()
            + "|" + policy.openDuration() + "|" + policy.halfOpenMaxPermits() + "|"
            + policy.halfOpenRetryDelay() + "|" + policy.halfOpenProbeLeaseDuration();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte valueByte : digest) {
                result.append(String.format("%02x", valueByte));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static String partitionKey(CircuitIdentity identity) {
        return "CIRCUIT#" + identity.value();
    }

    private static String probeSortKey(long epoch, int slot) {
        return "PROBE#" + epoch + "#" + slot;
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String text(AttributeValue value) {
        return value == null || value.s() == null ? "" : value.s();
    }

    private static long numeric(AttributeValue value) {
        return value == null || value.n() == null ? 0L : Long.parseLong(value.n());
    }

    private static long openUntilEpochMs(Instant openUntil) {
        return Instant.MIN.equals(openUntil) ? 0L : openUntil.toEpochMilli();
    }

    private record StateRecord(SharedCircuitSnapshot snapshot, String fingerprint, Map<Long, Integer> buckets) {
        private StateRecord {
            Objects.requireNonNull(snapshot, "snapshot must not be null");
            Objects.requireNonNull(fingerprint, "fingerprint must not be null");
            buckets = Map.copyOf(Objects.requireNonNull(buckets, "buckets must not be null"));
        }
    }
}
