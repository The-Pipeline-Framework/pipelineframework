package org.pipelineframework.awaitable.admission;

import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.locks.ReentrantLock;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.awaitable.admission.AwaitAdmissionAcquireResult;
import org.pipelineframework.awaitable.admission.AwaitAdmissionOwner;
import org.pipelineframework.awaitable.admission.AwaitAdmissionReservation;
import org.pipelineframework.awaitable.admission.AwaitAdmissionScope;
import org.pipelineframework.awaitable.admission.AwaitAdmissionStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;

/**
 * DynamoDB fixed-slot admission store. Claims and releases are conditional writes;
 * no mutable counter is maintained.
 */
@ApplicationScoped
public class DynamoAwaitAdmissionStore implements AwaitAdmissionStore {
    private static final String SCOPE_KEY = "scope_key";
    private static final String SLOT = "slot";
    private static final String OWNER_KEY = "owner_key";
    private static final String EXPIRES_AT = "expires_at";
    private static final String LEASE_TOKEN = "lease_token";

    @Inject
    PipelineOrchestratorConfig orchestratorConfig;

    private volatile DynamoDbClient client;
    private final Map<String, OwnerLock> ownerLocks = new HashMap<>();

    DynamoAwaitAdmissionStore() {
    }

    DynamoAwaitAdmissionStore(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.client = client;
        this.orchestratorConfig = orchestratorConfig;
    }

    @Override
    public String providerName() {
        return "dynamo";
    }

    @Override
    public CompletionStage<AwaitAdmissionAcquireResult> acquire(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        int capacity,
        long expiresAtEpochMs,
        long nowEpochMs
    ) {
        return blocking(() -> withOwnerLock(
            scope,
            owner,
            () -> acquireBlocking(scope, owner, capacity, expiresAtEpochMs, nowEpochMs)));
    }

    @Override
    public CompletionStage<Boolean> release(AwaitAdmissionReservation reservation) {
        return blocking(() -> {
            try {
                dynamoClient().deleteItem(DeleteItemRequest.builder()
                    .tableName(table())
                    .key(key(reservation.scope(), reservation.slot()))
                    .conditionExpression("#owner = :owner AND #lease = :lease")
                    .expressionAttributeNames(Map.of("#owner", OWNER_KEY, "#lease", LEASE_TOKEN))
                    .expressionAttributeValues(Map.of(":owner", string(reservation.owner().key()), ":lease", string(reservation.leaseToken())))
                    .build());
                return true;
            } catch (ConditionalCheckFailedException ignored) {
                return false;
            }
        });
    }

    private AwaitAdmissionAcquireResult acquireBlocking(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        int capacity,
        long expiresAtEpochMs,
        long nowEpochMs
    ) {
        return acquireBlocking(scope, owner, capacity, expiresAtEpochMs, nowEpochMs, 0);
    }

    private AwaitAdmissionAcquireResult acquireBlocking(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        int capacity,
        long expiresAtEpochMs,
        long nowEpochMs,
        int collisionRetries
    ) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        long nowSeconds = Math.max(1, nowEpochMs / 1000);
        Map<Integer, Map<String, AttributeValue>> reservationsBySlot = reservationsBySlot(scope);
        boolean reconciledExpired = false;
        for (int offset = 0; offset < capacity; offset++) {
            int slot = Math.floorMod(owner.key().hashCode() + offset, capacity);
            Map<String, AttributeValue> existing = reservationsBySlot.getOrDefault(slot, Map.of());
            if (!existing.isEmpty() && owner.key().equals(text(existing.get(OWNER_KEY)))
                && hasLeaseToken(existing) && number(existing.get(EXPIRES_AT)) > nowSeconds) {
                return AwaitAdmissionAcquireResult.reused(new AwaitAdmissionReservation(
                    scope, owner, slot, number(existing.get(EXPIRES_AT)) * 1000, text(existing.get(LEASE_TOKEN))));
            }
            if (!existing.isEmpty() && number(existing.get(EXPIRES_AT)) > nowSeconds) {
                continue;
            }
            reconciledExpired = reconciledExpired || !existing.isEmpty();
            try {
                AwaitAdmissionReservation reservation = new AwaitAdmissionReservation(scope, owner, slot, expiresAtEpochMs);
                dynamoClient().putItem(PutItemRequest.builder()
                    .tableName(table())
                    .item(Map.of(
                        SCOPE_KEY, string(scope.key()),
                        SLOT, number(slot),
                        OWNER_KEY, string(owner.key()),
                        LEASE_TOKEN, string(reservation.leaseToken()),
                        EXPIRES_AT, number(Math.max(nowSeconds + 1, expiresAtEpochMs / 1000))))
                    .conditionExpression("attribute_not_exists(#scope) OR #expires < :now")
                    .expressionAttributeNames(Map.of("#scope", SCOPE_KEY, "#expires", EXPIRES_AT))
                    .expressionAttributeValues(Map.of(":now", number(nowSeconds)))
                    .build());
                return reconciledExpired
                    ? AwaitAdmissionAcquireResult.acquiredAfterReconciliation(reservation)
                    : AwaitAdmissionAcquireResult.acquired(reservation);
            } catch (ConditionalCheckFailedException ignored) {
                // Re-probe from the deterministic first slot. A concurrent claim by this
                // owner is then observed as a reuse instead of allowing it to claim a
                // second slot; bounded retries preserve unavailable behavior under churn.
                if (collisionRetries < capacity) {
                    return acquireBlocking(scope, owner, capacity, expiresAtEpochMs, nowEpochMs, collisionRetries + 1);
                }
                return AwaitAdmissionAcquireResult.unavailable();
            }
        }
        return AwaitAdmissionAcquireResult.unavailable();
    }

    /**
     * Reads the fixed slot set for a scope in one strongly-consistent request.  A full budget is
     * a normal waiting state, so issuing one request per slot here would otherwise make every
     * waiting live unit repeatedly saturate the shared blocking executor.
     */
    private Map<Integer, Map<String, AttributeValue>> reservationsBySlot(AwaitAdmissionScope scope) {
        List<Map<String, AttributeValue>> items = dynamoClient().query(QueryRequest.builder()
            .tableName(table())
            .keyConditionExpression("#scope = :scope")
            .expressionAttributeNames(Map.of("#scope", SCOPE_KEY))
            .expressionAttributeValues(Map.of(":scope", string(scope.key())))
            .consistentRead(true)
            .build()).items();
        Map<Integer, Map<String, AttributeValue>> result = new HashMap<>();
        for (Map<String, AttributeValue> item : items) {
            AttributeValue slot = item.get(SLOT);
            if (slot != null) {
                result.put((int) number(slot), item);
            }
        }
        return result;
    }

    /**
     * Serializes local requests for the same provider scope and owner. Conditional claims
     * remain the cross-replica guard; this prevents one runtime from probing past its own
     * just-created claim before it can be observed as a reuse.
     */
    private <T> T withOwnerLock(
        AwaitAdmissionScope scope,
        AwaitAdmissionOwner owner,
        java.util.function.Supplier<T> operation
    ) {
        String lockKey = AwaitAdmissionScope.lengthPrefixedKey(scope.key(), owner.key());
        OwnerLock ownerLock;
        synchronized (ownerLocks) {
            ownerLock = ownerLocks.computeIfAbsent(lockKey, ignored -> new OwnerLock());
            ownerLock.retain();
        }
        ownerLock.lock();
        try {
            return operation.get();
        } finally {
            ownerLock.unlock();
            synchronized (ownerLocks) {
                if (ownerLock.release()) {
                    ownerLocks.remove(lockKey, ownerLock);
                }
            }
        }
    }

    private static <T> CompletionStage<T> blocking(java.util.function.Supplier<T> supplier) {
        return Uni.createFrom().item(supplier)
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .subscribeAsCompletionStage();
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            if (client == null) {
                var builder = DynamoDbClient.builder().httpClientBuilder(UrlConnectionHttpClient.builder());
                orchestratorConfig.dynamo().region().filter(region -> !region.isBlank()).ifPresent(region -> builder.region(Region.of(region)));
                orchestratorConfig.dynamo().endpointOverride().filter(endpoint -> !endpoint.isBlank())
                    .ifPresent(endpoint -> builder.endpointOverride(URI.create(endpoint)));
                client = builder.build();
            }
            return client;
        }
    }

    private String table() {
        return orchestratorConfig.dynamo().awaitAdmissionTable();
    }

    private static Map<String, AttributeValue> key(AwaitAdmissionScope scope, int slot) {
        return Map.of(SCOPE_KEY, string(scope.key()), SLOT, number(slot));
    }

    private static AttributeValue string(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue number(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String text(AttributeValue value) {
        return value == null ? "" : value.s();
    }

    private static long number(AttributeValue value) {
        return value == null || value.n() == null ? 0L : Long.parseLong(value.n());
    }

    private static boolean hasLeaseToken(Map<String, AttributeValue> item) {
        return item.containsKey(LEASE_TOKEN) && !text(item.get(LEASE_TOKEN)).isBlank();
    }

    private static final class OwnerLock {
        private final ReentrantLock lock = new ReentrantLock();
        private int references;

        void retain() {
            references++;
        }

        void lock() {
            lock.lock();
        }

        void unlock() {
            lock.unlock();
        }

        boolean release() {
            references--;
            return references == 0;
        }
    }

    @PreDestroy
    void close() {
        DynamoDbClient active = client;
        if (active != null) {
            active.close();
        }
    }
}
