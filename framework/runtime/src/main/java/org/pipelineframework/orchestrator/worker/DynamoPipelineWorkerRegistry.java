package org.pipelineframework.orchestrator.worker;

import java.net.URI;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * DynamoDB-backed worker lifecycle registry using append-only event records.
 */
public class DynamoPipelineWorkerRegistry implements PipelineWorkerRegistry {

    private static final String REGISTRY_KEY = "registry_key";
    private static final String REGISTRY_SORT = "registry_sort";
    private static final String RECORD_TYPE = "record_type";
    private static final String RECORD_TYPE_EVENT = "worker_event";
    private static final String EVENT_TYPE = "event_type";
    private static final String EVENT_REGISTER = "REGISTER";
    private static final String EVENT_HEARTBEAT = "HEARTBEAT";
    private static final String EVENT_DRAIN = "DRAIN";
    private static final String TENANT_ID = "tenant_id";
    private static final String PIPELINE_ID = "pipeline_id";
    private static final String CONTRACT_VERSION = "contract_version";
    private static final String RELEASE_VERSION = "release_version";
    private static final String WORKER_ID = "worker_id";
    private static final String PROTOCOL = "protocol";
    private static final String ENDPOINT = "endpoint";
    private static final String ARTIFACT_ID = "artifact_id";
    private static final String ARTIFACT_DIGEST = "artifact_digest";
    private static final String EVENT_AT_EPOCH_MS = "event_at_epoch_ms";

    private final DynamoDbClient explicitClient;
    private final PipelineOrchestratorConfig explicitConfig;
    private volatile DynamoDbClient client;
    private volatile PipelineOrchestratorConfig orchestratorConfig;

    public DynamoPipelineWorkerRegistry() {
        this(null, null);
    }

    public DynamoPipelineWorkerRegistry(PipelineOrchestratorConfig orchestratorConfig) {
        this(null, orchestratorConfig);
    }

    DynamoPipelineWorkerRegistry(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.explicitClient = client;
        this.explicitConfig = orchestratorConfig;
        this.client = client;
    }

    @Override
    public Uni<PipelineWorkerRecord> register(PipelineWorkerRegistration registration, long nowEpochMs) {
        return blocking(() -> {
            putEvent(eventItem(registration, EVENT_REGISTER, nowEpochMs));
            return currentWorker(registration.tenantId(), registration.pipelineId(), registration.workerId(), nowEpochMs, Duration.ZERO)
                .orElseThrow(() -> new IllegalStateException("Worker registration event was not persisted"));
        });
    }

    @Override
    public Uni<Optional<PipelineWorkerRecord>> heartbeat(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter) {
        return blocking(() -> {
            Optional<PipelineWorkerRecord> current = currentWorker(tenantId, pipelineId, workerId, nowEpochMs, staleAfter);
            if (current.isEmpty()) {
                return Optional.empty();
            }
            putEvent(eventItem(current.get(), EVENT_HEARTBEAT, nowEpochMs));
            return currentWorker(tenantId, pipelineId, workerId, nowEpochMs, staleAfter);
        });
    }

    @Override
    public Uni<Optional<PipelineWorkerRecord>> markDraining(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter) {
        return blocking(() -> {
            Optional<PipelineWorkerRecord> current = currentWorker(tenantId, pipelineId, workerId, nowEpochMs, staleAfter);
            if (current.isEmpty()) {
                return Optional.empty();
            }
            putEvent(eventItem(current.get(), EVENT_DRAIN, nowEpochMs));
            return currentWorker(tenantId, pipelineId, workerId, nowEpochMs, staleAfter);
        });
    }

    @Override
    public Uni<List<PipelineWorkerRecord>> list(
        String tenantId,
        String pipelineId,
        long nowEpochMs,
        Duration staleAfter) {
        return blocking(() -> currentWorkers(tenantId, pipelineId, Optional.empty(), nowEpochMs, staleAfter));
    }

    private Optional<PipelineWorkerRecord> currentWorker(
        String tenantId,
        String pipelineId,
        String workerId,
        long nowEpochMs,
        Duration staleAfter) {
        return currentWorkers(tenantId, pipelineId, Optional.of(workerId), nowEpochMs, staleAfter)
            .stream()
            .filter(record -> record.workerId().equals(workerId))
            .findFirst();
    }

    private List<PipelineWorkerRecord> currentWorkers(
        String tenantId,
        String pipelineId,
        Optional<String> workerId,
        long nowEpochMs,
        Duration staleAfter) {
        QueryResponse response = dynamoClient().query(QueryRequest.builder()
            .tableName(workerTable())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
            .expressionAttributeValues(Map.of(
                ":pk", avS(partitionKey(tenantId, pipelineId)),
                ":prefix", avS(workerId.map(DynamoPipelineWorkerRegistry::workerSortPrefix).orElse("worker:"))))
            .scanIndexForward(true)
            .build());
        Map<String, WorkerAccumulator> workers = new HashMap<>();
        for (Map<String, AttributeValue> item : response.items()) {
            WorkerAccumulator accumulator = workers.computeIfAbsent(stringValue(item, WORKER_ID), WorkerAccumulator::new);
            accumulator.apply(item);
        }
        return workers.values().stream()
            .map(accumulator -> accumulator.toRecord(nowEpochMs, staleAfter))
            .flatMap(Optional::stream)
            .sorted(Comparator.comparing(PipelineWorkerRecord::workerId))
            .toList();
    }

    private void putEvent(Map<String, AttributeValue> item) {
        dynamoClient().putItem(PutItemRequest.builder()
            .tableName(workerTable())
            .item(item)
            .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
            .build());
    }

    private Map<String, AttributeValue> eventItem(
        PipelineWorkerRegistration registration,
        String eventType,
        long nowEpochMs) {
        return eventItem(
            registration.tenantId(),
            registration.pipelineId(),
            registration.contractVersion(),
            registration.releaseVersion(),
            registration.workerId(),
            registration.protocol(),
            registration.endpoint(),
            registration.artifactId(),
            registration.artifactDigest(),
            eventType,
            nowEpochMs);
    }

    private Map<String, AttributeValue> eventItem(
        PipelineWorkerRecord record,
        String eventType,
        long nowEpochMs) {
        return eventItem(
            record.tenantId(),
            record.pipelineId(),
            record.contractVersion(),
            record.releaseVersion(),
            record.workerId(),
            record.protocol(),
            record.endpoint(),
            record.artifactId(),
            record.artifactDigest(),
            eventType,
            nowEpochMs);
    }

    private Map<String, AttributeValue> eventItem(
        String tenantId,
        String pipelineId,
        String contractVersion,
        String releaseVersion,
        String workerId,
        String protocol,
        String endpoint,
        String artifactId,
        String artifactDigest,
        String eventType,
        long nowEpochMs) {
        return Map.ofEntries(
            Map.entry(REGISTRY_KEY, avS(partitionKey(tenantId, pipelineId))),
            Map.entry(REGISTRY_SORT, avS(workerSort(workerId, nowEpochMs, eventType))),
            Map.entry(RECORD_TYPE, avS(RECORD_TYPE_EVENT)),
            Map.entry(EVENT_TYPE, avS(eventType)),
            Map.entry(TENANT_ID, avS(tenantId)),
            Map.entry(PIPELINE_ID, avS(pipelineId)),
            Map.entry(CONTRACT_VERSION, avS(contractVersion)),
            Map.entry(RELEASE_VERSION, avS(releaseVersion)),
            Map.entry(WORKER_ID, avS(workerId)),
            Map.entry(PROTOCOL, avS(protocol)),
            Map.entry(ENDPOINT, avS(endpoint)),
            Map.entry(ARTIFACT_ID, avS(artifactId)),
            Map.entry(ARTIFACT_DIGEST, avS(artifactDigest)),
            Map.entry(EVENT_AT_EPOCH_MS, avN(nowEpochMs)));
    }

    private DynamoDbClient dynamoClient() {
        DynamoDbClient active = client;
        if (active != null) {
            return active;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                active = newClient(config());
                client = active;
            }
            return active;
        }
    }

    private PipelineOrchestratorConfig config() {
        if (orchestratorConfig != null) {
            return orchestratorConfig;
        }
        if (explicitConfig != null) {
            return explicitConfig;
        }
        throw new IllegalStateException("Dynamo worker registry requires PipelineOrchestratorConfig");
    }

    private String workerTable() {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config().dynamo();
        if (dynamo == null || dynamo.workerTable() == null || dynamo.workerTable().isBlank()) {
            throw new IllegalStateException("pipeline.orchestrator.dynamo.worker-table must not be blank");
        }
        return dynamo.workerTable();
    }

    private static DynamoDbClient newClient(PipelineOrchestratorConfig config) {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config.dynamo();
        if (dynamo == null) {
            throw new IllegalStateException("Dynamo worker registry requires pipeline.orchestrator.dynamo.* configuration");
        }
        var builder = DynamoDbClient.builder()
            .httpClientBuilder(UrlConnectionHttpClient.builder());
        dynamo.region().filter(value -> !value.isBlank())
            .map(Region::of)
            .ifPresent(builder::region);
        dynamo.endpointOverride().filter(value -> !value.isBlank())
            .map(URI::create)
            .ifPresent(builder::endpointOverride);
        return builder.build();
    }

    private static String partitionKey(String tenantId, String pipelineId) {
        return tenantId + "#" + pipelineId;
    }

    private static String workerSortPrefix(String workerId) {
        return "worker:" + workerId + ":";
    }

    private static String workerSort(String workerId, long nowEpochMs, String eventType) {
        return workerSortPrefix(workerId) + String.format("%019d", nowEpochMs) + ":" + eventType;
    }

    private static AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private static AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private static String stringValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.s() == null) {
            throw new IllegalStateException("Dynamo worker registry record is missing string attribute " + name);
        }
        return value.s();
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new IllegalStateException("Dynamo worker registry record is missing numeric attribute " + name);
        }
        return Long.parseLong(value.n());
    }

    private static <T> Uni<T> blocking(java.util.function.Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private static final class WorkerAccumulator {
        private final String workerId;
        private String tenantId = "";
        private String pipelineId = "";
        private String contractVersion = "";
        private String releaseVersion = "";
        private String protocol = "";
        private String endpoint = "";
        private String artifactId = "";
        private String artifactDigest = "";
        private long registeredAtEpochMs;
        private long lastHeartbeatAtEpochMs;
        private long drainingSinceEpochMs;

        private WorkerAccumulator(String workerId) {
            this.workerId = workerId;
        }

        private void apply(Map<String, AttributeValue> item) {
            String eventType = stringValue(item, EVENT_TYPE);
            long eventAt = longValue(item, EVENT_AT_EPOCH_MS);
            if (EVENT_REGISTER.equals(eventType)) {
                tenantId = stringValue(item, TENANT_ID);
                pipelineId = stringValue(item, PIPELINE_ID);
                contractVersion = stringValue(item, CONTRACT_VERSION);
                releaseVersion = stringValue(item, RELEASE_VERSION);
                protocol = stringValue(item, PROTOCOL);
                endpoint = stringValue(item, ENDPOINT);
                artifactId = stringValue(item, ARTIFACT_ID);
                artifactDigest = stringValue(item, ARTIFACT_DIGEST);
                registeredAtEpochMs = eventAt;
                lastHeartbeatAtEpochMs = eventAt;
            } else if (EVENT_HEARTBEAT.equals(eventType)) {
                lastHeartbeatAtEpochMs = Math.max(lastHeartbeatAtEpochMs, eventAt);
            } else if (EVENT_DRAIN.equals(eventType)) {
                drainingSinceEpochMs = eventAt;
            }
        }

        private Optional<PipelineWorkerRecord> toRecord(long nowEpochMs, Duration staleAfter) {
            if (registeredAtEpochMs <= 0L) {
                return Optional.empty();
            }
            PipelineWorkerState state;
            if (drainingSinceEpochMs > 0L) {
                state = PipelineWorkerState.DRAINING;
            } else {
                long staleAfterMs = staleAfter == null ? Duration.ofMinutes(2).toMillis() : staleAfter.toMillis();
                state = staleAfterMs > 0L && nowEpochMs - lastHeartbeatAtEpochMs > staleAfterMs
                    ? PipelineWorkerState.STALE
                    : PipelineWorkerState.HEALTHY;
            }
            return Optional.of(new PipelineWorkerRecord(
                tenantId,
                pipelineId,
                contractVersion,
                releaseVersion,
                workerId,
                protocol,
                endpoint,
                artifactId,
                artifactDigest,
                state,
                registeredAtEpochMs,
                lastHeartbeatAtEpochMs,
                drainingSinceEpochMs));
        }
    }
}
