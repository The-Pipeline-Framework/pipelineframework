package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.enterprise.util.TypeLiteral;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitContinuationStatus;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitResumeTokenService;
import org.pipelineframework.awaitable.AwaitStepDescriptorFactory;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.DynamoAwaitInteractionStore;
import org.pipelineframework.awaitable.store.DynamoAwaitUnitStore;
import org.pipelineframework.orchestrator.DynamoExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionStatus;
import org.pipelineframework.stream.OpaqueSourceCheckpoint;
import org.pipelineframework.stream.StreamRegionContinuationRegistry;
import org.pipelineframework.stream.StreamRegionContinuationPayloadTest.DeterministicGeneratedFacade;
import org.pipelineframework.stream.StreamRegionContinuationPayloadTest.GeneratedInput;
import org.pipelineframework.stream.StreamRegionContinuationPayloadTest.GeneratedPage;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Durable feedback proof: a coordinator loss after the first atomic page commit leaves linked
 * interactions WAITING, a fresh sweep redrives them, and the bounded region reaches completion.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DynamoStreamRegionRedispatchIT {

  private static final String PREFIX = "stream_region_redispatch";

  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
      DockerImageName.parse("localstack/localstack:3.8"))
      .withServices(LocalStackContainer.Service.DYNAMODB);

  private DynamoDbClient dynamo;

  @BeforeAll
  void setUp() {
    dynamo = DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
        .region(Region.of(LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .build();
    createTable(PREFIX + "_execution", "tenant_id", "execution_id");
    createTable(PREFIX + "_execution_key", "tenant_execution_key", null);
    createTable(PREFIX + "_execution_payload", "payload_id", "payload_part");
    createTable(PREFIX + "_unit", "tenant_id", "unit_id");
    createInteractionTable();
    createTable(PREFIX + "_interaction_key", "lookup_key", null);
  }

  @AfterAll
  void tearDown() {
    if (dynamo != null) {
      dynamo.close();
    }
  }

  @Test
  void freshSweepRediscoversFirstCommittedPageBeforeAnyProviderCompletion() {
    PipelineOrchestratorConfig config = config();
    DeterministicGeneratedFacade continuation = new DeterministicGeneratedFacade();
    ExecutionStateStore executionStore = executionStore(config);
    DynamoAwaitInteractionStore interactions = interactionStore(config);
    AwaitCoordinator crashingCoordinator = coordinator(interactions, unitStore(config), config,
        new RecordingAdapter(), true);
    String executionId = executionStore.createOrGetExecution(new ExecutionCreateCommand(
        "tenant", "stream-seven", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "payments"),
        ExecutionResultShape.SINGLE, 1_000L, Long.MAX_VALUE)).await().indefinitely().record().executionId();
    StreamRegionRecord region = new StreamRegionRecord(
        "tenant", executionId, "payments-region", continuation.descriptor(), OpaqueSourceCheckpoint.initial(),
        0L, 0, 2, StreamRegionStatus.ACTIVE, Optional.empty(), 0L, "", 0L, 1_000L, 1_000L, 1_000L, Long.MAX_VALUE);
    executionStore.createStreamRegion(region).await().indefinitely().orElseThrow();

    StreamRegionFlow flow = new StreamRegionFlow(executionStore, crashingCoordinator, registry(continuation),
        new TransitionWorkerExecutor(null, new org.pipelineframework.invocation.PipelineInvocationRuntime()),
        new JsonTransitionPayloadCodec(), config);
    assertThrows(RuntimeException.class, () -> flow.process(
        ExecutionWorkItem.streamRegion("tenant", executionId, "payments-region"), worker(continuation))
        .await().indefinitely());

    StreamRegionRecord committed = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals("2", committed.checkpoint().value().orElseThrow());
    assertEquals(2L, committed.nextLogicalOrdinal());
    assertEquals(2, committed.outstandingCredits());
    assertEquals(StreamRegionStatus.ACTIVE, committed.status());
    assertEquals(Long.MAX_VALUE, committed.nextDueEpochMs());

    String firstId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 0);
    String secondId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 1);
    AwaitInteractionRecord first = interactions.get("tenant", firstId).await().indefinitely().orElseThrow();
    AwaitInteractionRecord second = interactions.get("tenant", secondId).await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.WAITING, first.status());
    assertEquals(AwaitInteractionStatus.WAITING, second.status());
    assertEquals(0, first.itemIndex());
    assertEquals(1, second.itemIndex());
    assertEquals("payments-region", first.streamRegionId());

    RecordingAdapter freshAdapter = new RecordingAdapter();
    AwaitCoordinator freshCoordinator = coordinator(interactionStore(config), unitStore(config), config,
        freshAdapter, false);
    RecordingDispatcher dispatcher = new RecordingDispatcher();
    QueueAsyncSweepFlow sweep = new QueueAsyncSweepFlow(config, executionStore, dispatcher,
        new AwaitTimeoutFlow(freshCoordinator, executionStore, SegmentBoundaryLedger::new), freshCoordinator,
        registry(continuation));
    sweep.sweepOnce(System.currentTimeMillis()).await().indefinitely();

    assertEquals(Set.of(firstId, secondId), Set.copyOf(freshAdapter.dispatchedInteractionIds()));
    assertEquals(AwaitInteractionStatus.DISPATCHED,
        interactions.get("tenant", firstId).await().indefinitely().orElseThrow().status());
    assertEquals(AwaitInteractionStatus.DISPATCHED,
        interactions.get("tenant", secondId).await().indefinitely().orElseThrow().status());
    StreamRegionRecord afterSweep = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(2L, afterSweep.nextLogicalOrdinal());
    assertEquals(2, afterSweep.outstandingCredits());
    assertTrue(dispatcher.items().stream().noneMatch(ExecutionWorkItem::streamRegion));

    AwaitBoundaryAdmission admission = new AwaitBoundaryAdmission(
        config, new ExecutionInputPolicy(), ignored -> org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision.allow(),
        freshCoordinator, null, () -> "pipeline", () -> "release", SegmentBoundaryLedger::new, null, dispatcher);
    AwaitItemContinuationHandler scalarSuffix = scalarSuffix();
    DurableAwaitItemContinuationFlow continuationFlow = new DurableAwaitItemContinuationFlow(
        freshCoordinator, dispatcher, executionStore);

    admission.complete(completion(secondId, "completion-1"), scalarSuffix).await().indefinitely();
    continuationFlow.process(ExecutionWorkItem.awaitContinuation("tenant", executionId, secondId), scalarSuffix)
        .await().indefinitely();
    StreamRegionRecord oneCreditReturned = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(1, oneCreditReturned.outstandingCredits());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactions.get("tenant", secondId).await().indefinitely().orElseThrow().continuationStatus());

    admission.complete(completion(firstId, "completion-0"), scalarSuffix).await().indefinitely();
    continuationFlow.process(ExecutionWorkItem.awaitContinuation("tenant", executionId, firstId), scalarSuffix)
        .await().indefinitely();
    StreamRegionRecord bothCreditsReturned = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(0, bothCreditsReturned.outstandingCredits());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactions.get("tenant", firstId).await().indefinitely().orElseThrow().continuationStatus());
    assertTrue(dispatcher.items().stream().anyMatch(ExecutionWorkItem::streamRegion));

    new StreamRegionFlow(executionStore, freshCoordinator, registry(continuation),
        new TransitionWorkerExecutor(null, new org.pipelineframework.invocation.PipelineInvocationRuntime()),
        new JsonTransitionPayloadCodec(), config)
        .process(ExecutionWorkItem.streamRegion("tenant", executionId, "payments-region"), worker(continuation))
        .await().indefinitely();
    StreamRegionRecord secondPage = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals("4", secondPage.checkpoint().value().orElseThrow());
    assertEquals(4L, secondPage.nextLogicalOrdinal());
    assertEquals(2, secondPage.outstandingCredits());

    String thirdId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 2);
    String fourthId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 3);
    apply(admission, continuationFlow, scalarSuffix, executionId, fourthId, "completion-3");
    StreamRegionRecord thirdCreditReturned = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(1, thirdCreditReturned.outstandingCredits());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactions.get("tenant", fourthId).await().indefinitely().orElseThrow().continuationStatus());

    apply(admission, continuationFlow, scalarSuffix, executionId, thirdId, "completion-2");
    StreamRegionRecord fourthCreditReturned = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(0, fourthCreditReturned.outstandingCredits());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactions.get("tenant", thirdId).await().indefinitely().orElseThrow().continuationStatus());

    new StreamRegionFlow(executionStore, freshCoordinator, registry(continuation),
        new TransitionWorkerExecutor(null, new org.pipelineframework.invocation.PipelineInvocationRuntime()),
        new JsonTransitionPayloadCodec(), config)
        .process(ExecutionWorkItem.streamRegion("tenant", executionId, "payments-region"), worker(continuation))
        .await().indefinitely();
    StreamRegionRecord thirdPage = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals("6", thirdPage.checkpoint().value().orElseThrow());
    assertEquals(6L, thirdPage.nextLogicalOrdinal());
    assertEquals(2, thirdPage.outstandingCredits());

    String fifthId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 4);
    String sixthId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 5);
    apply(admission, continuationFlow, scalarSuffix, executionId, sixthId, "completion-5");
    apply(admission, continuationFlow, scalarSuffix, executionId, fifthId, "completion-4");
    StreamRegionRecord sixthCreditReturned = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(0, sixthCreditReturned.outstandingCredits());

    new StreamRegionFlow(executionStore, freshCoordinator, registry(continuation),
        new TransitionWorkerExecutor(null, new org.pipelineframework.invocation.PipelineInvocationRuntime()),
        new JsonTransitionPayloadCodec(), config)
        .process(ExecutionWorkItem.streamRegion("tenant", executionId, "payments-region"), worker(continuation))
        .await().indefinitely();
    StreamRegionRecord sealedWithFinalItem = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals("7", sealedWithFinalItem.checkpoint().value().orElseThrow());
    assertEquals(7L, sealedWithFinalItem.nextLogicalOrdinal());
    assertEquals(1, sealedWithFinalItem.outstandingCredits());
    assertEquals(StreamRegionStatus.SOURCE_SEALED, sealedWithFinalItem.status());
    assertEquals(Optional.of(7L), sealedWithFinalItem.finalOrdinal());

    String seventhId = org.pipelineframework.orchestrator.stream.StreamRegionPageCommit.interactionId(region, 6);
    apply(admission, continuationFlow, scalarSuffix, executionId, seventhId, "completion-6");
    StreamRegionRecord completed = executionStore.getStreamRegion("tenant", executionId, "payments-region")
        .await().indefinitely().orElseThrow();
    assertEquals(0, completed.outstandingCredits());
    assertEquals(StreamRegionStatus.COMPLETED, completed.status());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactions.get("tenant", seventhId).await().indefinitely().orElseThrow().continuationStatus());
  }

  private static void apply(
      AwaitBoundaryAdmission admission,
      DurableAwaitItemContinuationFlow continuationFlow,
      AwaitItemContinuationHandler scalarSuffix,
      String executionId,
      String interactionId,
      String completionId
  ) {
    admission.complete(completion(interactionId, completionId), scalarSuffix).await().indefinitely();
    continuationFlow.process(ExecutionWorkItem.awaitContinuation("tenant", executionId, interactionId), scalarSuffix)
        .await().indefinitely();
  }

  private static AwaitCompletionCommand completion(String interactionId, String idempotencyKey) {
    return new AwaitCompletionCommand("tenant", interactionId, null, idempotencyKey,
        java.util.Map.of("approved", interactionId), "provider", System.currentTimeMillis());
  }

  private static AwaitItemContinuationHandler scalarSuffix() {
    return new AwaitItemContinuationHandler() {
      @Override
      public Uni<Void> continueAwaitItem(
          AwaitInteractionRecord record,
          org.pipelineframework.awaitable.AwaitUnitRecord unit,
          int nextStepIndex,
          Optional<org.pipelineframework.orchestrator.ExecutionRecord<Object, Object>> parent,
          long nowEpochMs) {
        return Uni.createFrom().voidItem();
      }

      @Override
      public Uni<Object> continueDurableAwaitItem(
          AwaitInteractionRecord record,
          org.pipelineframework.awaitable.AwaitUnitRecord unit,
          int nextStepIndex,
          Optional<org.pipelineframework.orchestrator.ExecutionRecord<Object, Object>> parent,
          long nowEpochMs) {
        return Uni.createFrom().item(record.responsePayload());
      }

      @Override
      public Uni<Void> releaseAwaitParentIfReady(
          org.pipelineframework.orchestrator.ExecutionRecord<Object, Object> parent,
          org.pipelineframework.awaitable.AwaitUnitRecord unit,
          int nextStepIndex,
          long nowEpochMs) {
        return Uni.createFrom().failure(new AssertionError("stream-linked continuation must not release an itemized parent"));
      }
    };
  }

  private PipelineTransitionWorker worker(DeterministicGeneratedFacade continuation) {
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
    return envelope -> {
      Object input = envelope.toCommand(codec).inputPayload();
      GeneratedInput generated = (GeneratedInput) ((ExecutionInputSnapshot) input).payload();
      return continuation.transitionStep().applyOneToOne(generated)
          .map(page -> TransitionResultEnvelope.completed(codec, List.of((GeneratedPage) page)));
    };
  }

  private AwaitCoordinator coordinator(
      AwaitInteractionStore interactionStore,
      AwaitUnitStore unitStore,
      PipelineOrchestratorConfig config,
      RecordingAdapter adapter,
      boolean crashBeforeDispatch
  ) {
    AwaitCoordinator coordinator = crashBeforeDispatch ? new CrashBeforeDispatchCoordinator() : new AwaitCoordinator();
    set(coordinator, "interactionStores", new TestInstance<>(List.of(interactionStore)));
    set(coordinator, "unitStores", new TestInstance<>(List.of(unitStore)));
    set(coordinator, "adapters", new TestInstance<>(List.of(adapter)));
    set(coordinator, "orchestratorConfig", config);
    set(coordinator, "resumeTokenService", construct(AwaitResumeTokenService.class,
        new Class<?>[] {String.class}, "stream-redispatch-test-secret"));
    set(coordinator, "descriptorFactory", new AwaitStepDescriptorFactory());
    return coordinator;
  }

  private StreamRegionContinuationRegistry registry(DeterministicGeneratedFacade continuation) {
    StreamRegionContinuationRegistry registry = new StreamRegionContinuationRegistry();
    set(registry, "continuations", new TestInstance<>(List.of(continuation)));
    return registry;
  }

  private ExecutionStateStore executionStore(PipelineOrchestratorConfig config) {
    return construct(DynamoExecutionStateStore.class,
        new Class<?>[] {DynamoDbClient.class, PipelineOrchestratorConfig.class}, dynamo, config);
  }

  private AwaitUnitStore unitStore(PipelineOrchestratorConfig config) {
    return construct(DynamoAwaitUnitStore.class,
        new Class<?>[] {DynamoDbClient.class, PipelineOrchestratorConfig.class}, dynamo, config);
  }

  private DynamoAwaitInteractionStore interactionStore(PipelineOrchestratorConfig config) {
    return construct(DynamoAwaitInteractionStore.class,
        new Class<?>[] {DynamoDbClient.class, PipelineOrchestratorConfig.class}, dynamo, config);
  }

  private PipelineOrchestratorConfig config() {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamoConfig = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamoConfig);
    when(dynamoConfig.executionTable()).thenReturn(PREFIX + "_execution");
    when(dynamoConfig.executionKeyTable()).thenReturn(PREFIX + "_execution_key");
    when(dynamoConfig.executionPayloadTable()).thenReturn(PREFIX + "_execution_payload");
    when(dynamoConfig.awaitUnitTable()).thenReturn(PREFIX + "_unit");
    when(dynamoConfig.awaitInteractionTable()).thenReturn(PREFIX + "_interaction");
    when(dynamoConfig.awaitInteractionKeyTable()).thenReturn(PREFIX + "_interaction_key");
    when(config.leaseMs()).thenReturn(60_000L);
    when(config.sweepLimit()).thenReturn(10);
    when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    return config;
  }

  private void createInteractionTable() {
    CreateTableRequest request = CreateTableRequest.builder()
        .tableName(PREFIX + "_interaction")
        .attributeDefinitions(
            attribute("tenant_id"), attribute("interaction_id"),
            attribute("query_continuation_due_key"), attribute("query_continuation_due_sort"),
            attribute("query_deadline_key"), attribute("query_deadline_sort"))
        .keySchema(key("tenant_id", KeyType.HASH), key("interaction_id", KeyType.RANGE))
        .globalSecondaryIndexes(
            index("await-interaction-continuation-by-due", "query_continuation_due_key", "query_continuation_due_sort"),
            index("await-interaction-pending-by-deadline", "query_deadline_key", "query_deadline_sort"))
        .provisionedThroughput(throughput())
        .build();
    dynamo.createTable(request);
    dynamo.waiter().waitUntilTableExists(waiter -> waiter.tableName(PREFIX + "_interaction"));
  }

  private void createTable(String name, String hash, String range) {
    List<AttributeDefinition> definitions = range == null ? List.of(attribute(hash)) : List.of(attribute(hash), attribute(range));
    List<KeySchemaElement> schema = range == null ? List.of(key(hash, KeyType.HASH))
        : List.of(key(hash, KeyType.HASH), key(range, KeyType.RANGE));
    dynamo.createTable(CreateTableRequest.builder().tableName(name).attributeDefinitions(definitions)
        .keySchema(schema).provisionedThroughput(throughput()).build());
    dynamo.waiter().waitUntilTableExists(waiter -> waiter.tableName(name));
  }

  private static AttributeDefinition attribute(String name) {
    return AttributeDefinition.builder().attributeName(name).attributeType(ScalarAttributeType.S).build();
  }

  private static KeySchemaElement key(String name, KeyType type) {
    return KeySchemaElement.builder().attributeName(name).keyType(type).build();
  }

  private static GlobalSecondaryIndex index(String name, String hash, String range) {
    return GlobalSecondaryIndex.builder().indexName(name).keySchema(key(hash, KeyType.HASH), key(range, KeyType.RANGE))
        .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).provisionedThroughput(throughput()).build();
  }

  private static ProvisionedThroughput throughput() {
    return ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build();
  }

  private static void set(Object target, String fieldName, Object value) {
    try {
      Field field = findField(target.getClass(), fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot configure test runtime field " + fieldName, failure);
    }
  }

  private static Field findField(Class<?> type, String fieldName) throws NoSuchFieldException {
    Class<?> current = type;
    while (current != null) {
      try {
        return current.getDeclaredField(fieldName);
      } catch (NoSuchFieldException ignored) {
        current = current.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }

  private static <T> T construct(Class<T> type, Class<?>[] argumentTypes, Object... arguments) {
    try {
      Constructor<T> constructor = type.getDeclaredConstructor(argumentTypes);
      constructor.setAccessible(true);
      return constructor.newInstance(arguments);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot construct " + type.getName(), failure);
    }
  }

  private static final class CrashBeforeDispatchCoordinator extends AwaitCoordinator {
    @Override
    public Uni<AwaitInteractionRecord> dispatch(org.pipelineframework.awaitable.AwaitStepDescriptor descriptor,
        AwaitInteractionRecord interaction) {
      return Uni.createFrom().failure(new IllegalStateException("simulated coordinator loss after page commit"));
    }
  }

  private static final class RecordingAdapter implements AwaitTransportAdapter<Object> {
    private final List<String> interactionIds = new CopyOnWriteArrayList<>();

    @Override
    public String type() {
      return "local";
    }

    @Override
    public Uni<AwaitDispatchResult> dispatch(AwaitDispatchRequest<Object> request) {
      interactionIds.add(request.interaction().interactionId());
      return Uni.createFrom().item(new AwaitDispatchResult(java.util.Map.of()));
    }

    List<String> dispatchedInteractionIds() {
      return List.copyOf(interactionIds);
    }
  }

  private static final class RecordingDispatcher implements WorkDispatcher {
    private final List<ExecutionWorkItem> items = new ArrayList<>();

    @Override
    public Uni<Void> enqueueNow(ExecutionWorkItem item) {
      items.add(item);
      return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Void> enqueueDelayed(ExecutionWorkItem item, java.time.Duration delay) {
      items.add(item);
      return Uni.createFrom().voidItem();
    }

    List<ExecutionWorkItem> items() {
      return List.copyOf(items);
    }
  }

  private static final class TestInstance<T> implements Instance<T> {
    private final List<T> values;

    private TestInstance(List<T> values) {
      this.values = List.copyOf(values);
    }

    @Override public Instance<T> select(Annotation... qualifiers) { throw new UnsupportedOperationException(); }
    @Override public <U extends T> Instance<U> select(Class<U> subtype, Annotation... qualifiers) { throw new UnsupportedOperationException(); }
    @Override public <U extends T> Instance<U> select(TypeLiteral<U> subtype, Annotation... qualifiers) { throw new UnsupportedOperationException(); }
    @Override public boolean isUnsatisfied() { return values.isEmpty(); }
    @Override public boolean isAmbiguous() { return values.size() > 1; }
    @Override public void destroy(T instance) { }
    @Override public Handle<T> getHandle() { throw new UnsupportedOperationException(); }
    @Override public Iterable<? extends Handle<T>> handles() { return List.of(); }
    @Override public java.util.Iterator<T> iterator() { return values.iterator(); }
    @Override public T get() { return values.stream().findFirst().orElseThrow(UnsatisfiedResolutionException::new); }
    @Override public java.util.stream.Stream<T> stream() { return values.stream(); }
  }
}
