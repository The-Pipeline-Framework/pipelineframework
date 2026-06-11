package org.pipelineframework.orchestrator.release;

import java.net.URI;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.annotation.PreDestroy;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import org.jboss.logging.Logger;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineBundleManifest;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionCheck;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * DynamoDB-backed release registry that stores immutable release and activation records.
 */
public class DynamoPipelineReleaseRegistry implements PipelineReleaseRegistry {
    private static final Logger LOG = Logger.getLogger(DynamoPipelineReleaseRegistry.class);

    private static final String REGISTRY_KEY = "registry_key";
    private static final String REGISTRY_SORT = "registry_sort";
    private static final String RECORD_TYPE = "record_type";
    private static final String RECORD_TYPE_RELEASE = "RELEASE";
    private static final String RECORD_TYPE_ACTIVATION = "ACTIVATION";
    private static final String RELEASE_PREFIX = "release:";
    private static final String ACTIVATION_PREFIX = "activation:";
    private static final String TENANT_ID = "tenant_id";
    private static final String PIPELINE_ID = "pipeline_id";
    private static final String CONTRACT_VERSION = "contract_version";
    private static final String RELEASE_VERSION = "release_version";
    private static final String BUNDLE_VERSION_ID = "bundle_version_id";
    private static final String BUNDLE_HASH = "bundle_hash";
    private static final String PRIMARY_ARTIFACT_PATH = "primary_artifact_path";
    private static final String PRIMARY_ARTIFACT_SIZE_BYTES = "primary_artifact_size_bytes";
    private static final String PRIMARY_ARTIFACT_CHECKSUM = "primary_artifact_checksum";
    private static final String DESCRIPTOR_JSON = "descriptor_json";
    private static final String MANIFEST_JSON = "manifest_json";
    private static final String CREATED_AT_EPOCH_MS = "created_at_epoch_ms";
    private static final String UPDATED_AT_EPOCH_MS = "updated_at_epoch_ms";
    private static final String ACTIVATED_AT_EPOCH_MS = "activated_at_epoch_ms";

    private final PipelineOrchestratorConfig explicitConfig;
    private volatile PipelineOrchestratorConfig orchestratorConfig;
    private volatile DynamoDbClient client;

    public DynamoPipelineReleaseRegistry() {
        this.explicitConfig = null;
    }

    public DynamoPipelineReleaseRegistry(PipelineOrchestratorConfig orchestratorConfig) {
        this.explicitConfig = orchestratorConfig;
        this.orchestratorConfig = orchestratorConfig;
    }

    DynamoPipelineReleaseRegistry(DynamoDbClient client, PipelineOrchestratorConfig orchestratorConfig) {
        this.explicitConfig = orchestratorConfig;
        this.orchestratorConfig = orchestratorConfig;
        this.client = client;
    }

    @Override
    public Uni<PipelineReleaseRecord> register(PipelineReleaseRecord record) {
        return blocking(() -> registerBlocking(record));
    }

    @Override
    public Uni<List<PipelineReleaseRecord>> list(String tenantId, String pipelineId) {
        return blocking(() -> listBlocking(tenantId, pipelineId));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> get(String tenantId, String pipelineId, String releaseVersion) {
        return blocking(() -> getBlocking(tenantId, pipelineId, releaseVersion));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> active(String tenantId, String pipelineId) {
        return blocking(() -> activeBlocking(tenantId, pipelineId));
    }

    @Override
    public Uni<Optional<PipelineReleaseRecord>> activate(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        return blocking(() -> activateBlocking(tenantId, pipelineId, releaseVersion, nowEpochMs));
    }

    @PreDestroy
    void closeClient() {
        DynamoDbClient active = client;
        if (active == null) {
            return;
        }
        synchronized (this) {
            active = client;
            if (active == null) {
                return;
            }
            try {
                active.close();
            } catch (Exception e) {
                LOG.debug("Failed closing DynamoDB client during shutdown.", e);
            } finally {
                client = null;
            }
        }
    }

    private PipelineReleaseRecord registerBlocking(PipelineReleaseRecord record) {
        Optional<PipelineReleaseRecord> existing = getBlocking(
            record.tenantId(),
            record.pipelineId(),
            record.releaseVersion());
        if (existing.isPresent()) {
            PipelineReleaseRecord current = existing.get();
            if (!sameImmutableMetadata(current, record)) {
                throw new IllegalStateException("Release version is already registered with different metadata");
            }
            return current;
        }
        try {
            dynamoClient().putItem(PutItemRequest.builder()
                .tableName(releaseTable())
                .item(toReleaseItem(record))
                .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
                .build());
            return record;
        } catch (ConditionalCheckFailedException ignored) {
            PipelineReleaseRecord current = getBlocking(record.tenantId(), record.pipelineId(), record.releaseVersion())
                .orElseThrow(() -> new IllegalStateException("Release registration lost race but no record was found"));
            if (!sameImmutableMetadata(current, record)) {
                throw new IllegalStateException("Release version is already registered with different metadata");
            }
            return current;
        }
    }

    private List<PipelineReleaseRecord> listBlocking(String tenantId, String pipelineId) {
        Optional<ActivationEvent> active = latestActivation(tenantId, pipelineId);
        return queryRecords(tenantId, pipelineId, RELEASE_PREFIX, true).items().stream()
            .filter(item -> RECORD_TYPE_RELEASE.equals(stringValue(item, RECORD_TYPE)))
            .map(item -> toReleaseRecord(item, active))
            .sorted(Comparator.comparingLong(PipelineReleaseRecord::createdAtEpochMs))
            .toList();
    }

    private Optional<PipelineReleaseRecord> getBlocking(String tenantId, String pipelineId, String releaseVersion) {
        return getReleaseRecord(tenantId, pipelineId, releaseVersion, latestActivation(tenantId, pipelineId));
    }

    private Optional<PipelineReleaseRecord> getReleaseRecord(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        Optional<ActivationEvent> active) {
        Map<String, AttributeValue> item = dynamoClient().getItem(GetItemRequest.builder()
            .tableName(releaseTable())
            .key(key(tenantId, pipelineId, releaseSort(releaseVersion)))
            .build()).item();
        if (item == null || item.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(toReleaseRecord(item, active));
    }

    private Optional<PipelineReleaseRecord> activeBlocking(String tenantId, String pipelineId) {
        Optional<ActivationEvent> activation = latestActivation(tenantId, pipelineId);
        if (activation.isEmpty()) {
            return Optional.empty();
        }
        return getReleaseRecord(tenantId, pipelineId, activation.get().releaseVersion(), activation)
            .map(record -> record.withStatus(PipelineReleaseStatus.ACTIVE, activation.get().activatedAtEpochMs()));
    }

    private Optional<PipelineReleaseRecord> activateBlocking(
        String tenantId,
        String pipelineId,
        String releaseVersion,
        long nowEpochMs) {
        Optional<PipelineReleaseRecord> existing = getReleaseRecord(tenantId, pipelineId, releaseVersion, Optional.empty());
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        ActivationEvent event = new ActivationEvent(releaseVersion, nowEpochMs);
        try {
            dynamoClient().transactWriteItems(TransactWriteItemsRequest.builder()
                .transactItems(
                    TransactWriteItem.builder().conditionCheck(ConditionCheck.builder()
                        .tableName(releaseTable())
                        .key(key(tenantId, pipelineId, releaseSort(releaseVersion)))
                        .conditionExpression("attribute_exists(#pk) AND attribute_exists(#sk)")
                        .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
                        .build()).build(),
                    TransactWriteItem.builder().put(Put.builder()
                        .tableName(releaseTable())
                        .item(toActivationItem(tenantId, pipelineId, event))
                        .conditionExpression("attribute_not_exists(#pk) AND attribute_not_exists(#sk)")
                        .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
                        .build()).build())
                .build());
        } catch (TransactionCanceledException | ConditionalCheckFailedException ignored) {
            return activeBlocking(tenantId, pipelineId)
                .filter(record -> record.releaseVersion().equals(releaseVersion));
        }
        return Optional.of(existing.get().withStatus(PipelineReleaseStatus.ACTIVE, nowEpochMs));
    }

    private Optional<ActivationEvent> latestActivation(String tenantId, String pipelineId) {
        return queryRecords(tenantId, pipelineId, ACTIVATION_PREFIX, false).items().stream()
            .filter(item -> RECORD_TYPE_ACTIVATION.equals(stringValue(item, RECORD_TYPE)))
            .findFirst()
            .map(item -> new ActivationEvent(
                stringValue(item, RELEASE_VERSION),
                longValue(item, ACTIVATED_AT_EPOCH_MS)));
    }

    private QueryResponse queryRecords(
        String tenantId,
        String pipelineId,
        String sortPrefix,
        boolean ascending) {
        return dynamoClient().query(QueryRequest.builder()
            .tableName(releaseTable())
            .keyConditionExpression("#pk = :pk AND begins_with(#sk, :prefix)")
            .expressionAttributeNames(Map.of("#pk", REGISTRY_KEY, "#sk", REGISTRY_SORT))
            .expressionAttributeValues(Map.of(
                ":pk", avS(partitionKey(tenantId, pipelineId)),
                ":prefix", avS(sortPrefix)))
            .scanIndexForward(ascending)
            .build());
    }

    private PipelineReleaseRecord toReleaseRecord(
        Map<String, AttributeValue> item,
        Optional<ActivationEvent> active) {
        String releaseVersion = stringValue(item, RELEASE_VERSION);
        long activatedAt = active
            .filter(event -> event.releaseVersion().equals(releaseVersion))
            .map(ActivationEvent::activatedAtEpochMs)
            .orElse(0L);
        PipelineReleaseStatus status = activatedAt > 0L
            ? PipelineReleaseStatus.ACTIVE
            : PipelineReleaseStatus.REGISTERED;
        return new PipelineReleaseRecord(
            stringValue(item, TENANT_ID),
            stringValue(item, PIPELINE_ID),
            stringValue(item, CONTRACT_VERSION),
            releaseVersion,
            status,
            fromJson(stringValue(item, DESCRIPTOR_JSON), PipelineReleaseDescriptor.class),
            stringValue(item, BUNDLE_VERSION_ID),
            stringValue(item, BUNDLE_HASH),
            stringValue(item, PRIMARY_ARTIFACT_PATH),
            longValue(item, PRIMARY_ARTIFACT_SIZE_BYTES),
            stringValue(item, PRIMARY_ARTIFACT_CHECKSUM),
            fromJson(stringValue(item, MANIFEST_JSON), PipelineBundleManifest.class),
            longValue(item, CREATED_AT_EPOCH_MS),
            longValue(item, UPDATED_AT_EPOCH_MS),
            activatedAt);
    }

    private Map<String, AttributeValue> toReleaseItem(PipelineReleaseRecord record) {
        return Map.ofEntries(
            Map.entry(REGISTRY_KEY, avS(partitionKey(record.tenantId(), record.pipelineId()))),
            Map.entry(REGISTRY_SORT, avS(releaseSort(record.releaseVersion()))),
            Map.entry(RECORD_TYPE, avS(RECORD_TYPE_RELEASE)),
            Map.entry(TENANT_ID, avS(record.tenantId())),
            Map.entry(PIPELINE_ID, avS(record.pipelineId())),
            Map.entry(CONTRACT_VERSION, avS(record.contractVersion())),
            Map.entry(RELEASE_VERSION, avS(record.releaseVersion())),
            Map.entry(BUNDLE_VERSION_ID, avS(record.bundleVersionId())),
            Map.entry(BUNDLE_HASH, avS(record.bundleHash())),
            Map.entry(PRIMARY_ARTIFACT_PATH, avS(record.primaryArtifactPath())),
            Map.entry(PRIMARY_ARTIFACT_SIZE_BYTES, avN(record.primaryArtifactSizeBytes())),
            Map.entry(PRIMARY_ARTIFACT_CHECKSUM, avS(record.primaryArtifactChecksum())),
            Map.entry(DESCRIPTOR_JSON, avS(toJson(record.descriptor()))),
            Map.entry(MANIFEST_JSON, avS(toJson(record.manifest()))),
            Map.entry(CREATED_AT_EPOCH_MS, avN(record.createdAtEpochMs())),
            Map.entry(UPDATED_AT_EPOCH_MS, avN(record.updatedAtEpochMs())),
            Map.entry(ACTIVATED_AT_EPOCH_MS, avN(record.activatedAtEpochMs())));
    }

    private Map<String, AttributeValue> toActivationItem(
        String tenantId,
        String pipelineId,
        ActivationEvent event) {
        return Map.ofEntries(
            Map.entry(REGISTRY_KEY, avS(partitionKey(tenantId, pipelineId))),
            Map.entry(REGISTRY_SORT, avS(activationSort(event))),
            Map.entry(RECORD_TYPE, avS(RECORD_TYPE_ACTIVATION)),
            Map.entry(TENANT_ID, avS(tenantId)),
            Map.entry(PIPELINE_ID, avS(pipelineId)),
            Map.entry(RELEASE_VERSION, avS(event.releaseVersion())),
            Map.entry(ACTIVATED_AT_EPOCH_MS, avN(event.activatedAtEpochMs())));
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
        PipelineOrchestratorConfig active = orchestratorConfig;
        if (active != null) {
            return active;
        }
        if (explicitConfig != null) {
            return explicitConfig;
        }
        throw new IllegalStateException("Dynamo release registry requires PipelineOrchestratorConfig");
    }

    private String releaseTable() {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config().dynamo();
        if (dynamo == null || dynamo.releaseTable() == null || dynamo.releaseTable().isBlank()) {
            throw new IllegalStateException("pipeline.orchestrator.dynamo.release-table must not be blank");
        }
        return dynamo.releaseTable();
    }

    private static DynamoDbClient newClient(PipelineOrchestratorConfig config) {
        PipelineOrchestratorConfig.DynamoConfig dynamo = config.dynamo();
        if (dynamo == null) {
            throw new IllegalStateException("Dynamo release registry requires pipeline.orchestrator.dynamo.* configuration");
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

    private static Map<String, AttributeValue> key(String tenantId, String pipelineId, String sortKey) {
        return Map.of(
            REGISTRY_KEY, avS(partitionKey(tenantId, pipelineId)),
            REGISTRY_SORT, avS(sortKey));
    }

    private static String partitionKey(String tenantId, String pipelineId) {
        return tenantId + "#" + pipelineId;
    }

    private static String releaseSort(String releaseVersion) {
        return RELEASE_PREFIX + releaseVersion;
    }

    private static String activationSort(ActivationEvent event) {
        return ACTIVATION_PREFIX + String.format("%019d", event.activatedAtEpochMs())
            + ":" + event.releaseVersion();
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
            throw new IllegalStateException("Dynamo release registry record is missing string attribute " + name);
        }
        return value.s();
    }

    private static long longValue(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        if (value == null || value.n() == null) {
            throw new IllegalStateException("Dynamo release registry record is missing numeric attribute " + name);
        }
        return Long.parseLong(value.n());
    }

    private static String toJson(Object value) {
        try {
            return PipelineJson.mapper().writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing release registry value", e);
        }
    }

    private static <T> T fromJson(String value, Class<T> type) {
        try {
            return PipelineJson.mapper().readValue(value, type);
        } catch (Exception e) {
            throw new IllegalStateException("Failed deserializing release registry value " + type.getName(), e);
        }
    }

    private static boolean sameImmutableMetadata(PipelineReleaseRecord left, PipelineReleaseRecord right) {
        return left.contractVersion().equals(right.contractVersion())
            && left.releaseVersion().equals(right.releaseVersion())
            && left.bundleVersionId().equals(right.bundleVersionId())
            && left.bundleHash().equals(right.bundleHash())
            && left.primaryArtifactPath().equals(right.primaryArtifactPath())
            && left.primaryArtifactSizeBytes() == right.primaryArtifactSizeBytes()
            && left.primaryArtifactChecksum().equals(right.primaryArtifactChecksum())
            && toJson(left.descriptor()).equals(toJson(right.descriptor()))
            && toJson(left.manifest()).equals(toJson(right.manifest()));
    }

    private static <T> Uni<T> blocking(java.util.function.Supplier<T> supplier) {
        return Uni.createFrom().item(supplier).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private record ActivationEvent(String releaseVersion, long activatedAtEpochMs) {
        private ActivationEvent {
            if (releaseVersion == null || releaseVersion.isBlank()) {
                throw new IllegalArgumentException("releaseVersion must not be blank");
            }
            if (activatedAtEpochMs <= 0) {
                throw new IllegalArgumentException("activatedAtEpochMs must be positive");
            }
        }
    }
}
