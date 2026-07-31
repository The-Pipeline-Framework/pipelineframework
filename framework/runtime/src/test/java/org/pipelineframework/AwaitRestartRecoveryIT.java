package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCompletionResult;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitLifecycleCoverageRegistry;
import org.pipelineframework.awaitable.AwaitUnitCreateCommand;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitUnitStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.orchestrator.CreateExecutionResult;
import org.pipelineframework.orchestrator.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * A fresh-runtime proof for the most failure-prone itemized recovery seam.  The broader registry
 * names this as {@code itemized_final_child_after_restart_releases_parent}.
 */
@Testcontainers(disabledWithoutDocker = true)
@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AwaitRestartRecoveryIT {

  private static final String TABLE_PREFIX = "await_lifecycle";

  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
      DockerImageName.parse("localstack/localstack:3.8"))
      .withServices(LocalStackContainer.Service.DYNAMODB);

  private DynamoDbClient dynamo;
  private ScheduledExecutorService scheduler;

  @BeforeAll
  void createTables() {
    dynamo = DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
        .region(Region.of(LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .build();
    createTable(TABLE_PREFIX + "_execution", "tenant_id", "execution_id");
    createTable(TABLE_PREFIX + "_execution_key", "tenant_execution_key", null);
    createTable(TABLE_PREFIX + "_execution_payload", "payload_id", "payload_part");
    createTable(TABLE_PREFIX + "_unit", "tenant_id", "unit_id");
    createTable(TABLE_PREFIX + "_interaction", "tenant_id", "interaction_id");
    createTable(TABLE_PREFIX + "_interaction_key", "lookup_key", null);
  }

  @AfterEach
  void stopScheduler() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  private ScheduledExecutorService replaceScheduler() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
    scheduler = Executors.newSingleThreadScheduledExecutor();
    return scheduler;
  }

  @AfterAll
  void closeDynamo() {
    dynamo.close();
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  @TestFactory
  Stream<DynamicTest> declaredDynamoRecoveryJourneys() {
    return Stream.of(
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_dispatch_after_restart_reconstructs_completion"),
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_completion_after_restart_resumes_once"),
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_duplicate_completion_race"),
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_conflicting_completion_race"),
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_timeout_completion_race"),
            AwaitLifecycleCoverageRegistry.journeyNamed("scalar_cancellation_completion_race"),
            AwaitLifecycleCoverageRegistry.journeyNamed("one_to_many_durable_shape_uninterrupted"),
            AwaitLifecycleCoverageRegistry.journeyNamed("one_to_many_durable_shape_after_restart"),
            AwaitLifecycleCoverageRegistry.journeyNamed("many_to_one_durable_shape_uninterrupted"),
            AwaitLifecycleCoverageRegistry.journeyNamed("many_to_one_durable_shape_after_restart"),
            AwaitLifecycleCoverageRegistry.journeyNamed("itemized_empty_unit_after_restart_terminalizes"),
            AwaitLifecycleCoverageRegistry.journeyNamed("itemized_final_child_after_restart_releases_parent"),
            AwaitLifecycleCoverageRegistry.journeyNamed("itemized_transition_admission_after_restart"),
            AwaitLifecycleCoverageRegistry.journeyNamed("many_to_many_partial_replay_holds_parent"),
            AwaitLifecycleCoverageRegistry.journeyNamed("sequential_durable_shape_uninterrupted"),
            AwaitLifecycleCoverageRegistry.journeyNamed("sequential_durable_shape_after_restart"),
            AwaitLifecycleCoverageRegistry.journeyNamed("terminal_cleanup_durable_state"))
        .map(journey -> DynamicTest.dynamicTest(journey.name(), () -> executeDynamoJourney(journey)));
  }

  private void executeDynamoJourney(AwaitLifecycleCoverageRegistry.Journey journey) {
    switch (journey.fixtureScenario()) {
      case "scalarDispatchRestart" -> dispatchedRequestCompletesAfterWorkerRestart();
      case "scalarRestart" -> scalarCompletionPersistsAcrossAWorkerRestart();
      case "duplicateCompletionRace" -> duplicateCompletionHasOneDurableWinner();
      case "conflictingCompletionRace" -> conflictingCompletionHasOneDurableWinner();
      case "timeoutCompletionRace" -> timeoutAndCompletionConverge();
      case "cancellationCompletionRace" -> cancellationAndCompletionConverge();
      case "oneToManyUninterrupted" -> oneToManyCompletesWithOneRequestAndManyOutputs(false);
      case "oneToManyRestart" -> oneToManyCompletesWithOneRequestAndManyOutputs(true);
      case "manyToOneUninterrupted" -> manyToOneCompletesAfterEveryInput(false);
      case "manyToOneRestart" -> manyToOneCompletesAfterEveryInput(true);
      case "emptyItemizedRestart" -> emptyItemizedUnitReleasesParentAfterAWorkerRestart();
      case "itemizedRestart" -> finalChildAfterRestartReleasesParentFromDurableChildren();
      case "transitionAdmissionRestart" -> recoversItemContinuationAfterWorkerDiesWithAnAdmittedTransition();
      case "partialItemReplay" -> partialDurableChildrenNeverReleaseParentFromLocalClaims();
      case "sequentialShapeUninterrupted" -> sequentialAwaitUnitsAdvanceInOrder(false);
      case "sequentialShapeRestart" -> sequentialAwaitUnitsAdvanceInOrder(true);
      case "terminalCleanup" -> terminalCleanupLeavesNoDurableAwaitWork();
      default -> throw new IllegalArgumentException("No Dynamo recovery fixture for " + journey.fixtureScenario());
    }
  }

  private void dispatchedRequestCompletesAfterWorkerRestart() {
    long now = System.currentTimeMillis();
    AwaitInteractionStore workerA = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStore(dynamo, TABLE_PREFIX);
    AwaitInteractionRecord requested = createInteraction(workerA, "tenant-dispatch-restart", "dispatch-restart", now);

    // This is the durable admission edge around the adapter call: the provider cannot be invoked
    // until the request is claimed, and its acknowledgement is recorded before worker A vanishes.
    AwaitInteractionRecord dispatching = workerA.markDispatching(
            requested.tenantId(), requested.interactionId(), requested.version(), now + 1L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.DISPATCHING, dispatching.status());
    AwaitInteractionRecord dispatched = workerA.markDispatched(
            dispatching.tenantId(), dispatching.interactionId(), dispatching.version(),
            Map.of("adapter", "lifecycle-test", "dispatchId", "dispatch-restart"), now + 2L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.DISPATCHED, dispatched.status());

    // Runtime B knows only the Dynamo interaction.  A later provider completion remains fully
    // admissible; no worker-local dispatch acknowledgement is required to reconstruct the contract.
    AwaitInteractionStore workerB = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, TABLE_PREFIX);
    AwaitInteractionRecord restored = workerB.get(dispatched.tenantId(), dispatched.interactionId())
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.DISPATCHED, restored.status());
    assertEquals("lifecycle-test", restored.transportMetadata().get("adapter"));
    AwaitCompletionResult completed = workerB.complete(new AwaitCompletionCommand(
            restored.tenantId(), restored.interactionId(), restored.correlationId(), "completion-dispatch-restart",
            Map.of("decision", "approved"), "provider", now + 3L))
        .await().indefinitely();
    assertFalse(completed.duplicate());
    assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());
  }

  private void oneToManyCompletesWithOneRequestAndManyOutputs(boolean freshRuntime) {
    long now = System.currentTimeMillis();
    String suffix = freshRuntime ? "restart" : "uninterrupted";
    String tenant = "tenant-one-to-many-" + suffix;
    AwaitUnitStore workerAUnits = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX);
    AwaitInteractionStore workerAInteractions = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStore(dynamo, TABLE_PREFIX);
    AwaitUnitRecord unit = createUnit(workerAUnits, tenant, "execution-one-to-many-" + suffix,
        "unit-one-to-many-" + suffix, 2, "ONE_TO_MANY", now);
    AwaitInteractionRecord request = createInteraction(workerAInteractions, tenant, "one-to-many-" + suffix, now);
    workerAUnits.markDispatchComplete(tenant, unit.unitId(), 1, now + 1L).await().indefinitely().orElseThrow();

    AwaitInteractionStore completer = freshRuntime
        ? org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, TABLE_PREFIX)
        : workerAInteractions;
    AwaitUnitStore aggregator = freshRuntime
        ? org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX)
        : workerAUnits;
    AwaitCompletionResult completion = completer.complete(new AwaitCompletionCommand(
            request.tenantId(), request.interactionId(), request.correlationId(), "completion-one-to-many-" + suffix,
            Map.of("outputs", List.of("approved", "published")), "provider", now + 2L))
        .await().indefinitely();
    assertFalse(completion.duplicate());
    AwaitUnitRecord completed = aggregator.recordItemCompleted(tenant, unit.unitId(), "request:0", now + 3L)
        .await().indefinitely().orElseThrow();
    assertEquals("ONE_TO_MANY", completed.cardinality());
    assertEquals(AwaitUnitStatus.COMPLETED, completed.status());
    Map<?, ?> outputPayload = assertInstanceOf(Map.class, completion.record().responsePayload());
    assertEquals(List.of("approved", "published"), outputPayload.get("outputs"));

    AwaitInteractionRecord raced = createInteraction(workerAInteractions, tenant, "one-to-many-timeout-" + suffix, now + 4L);
    List<RaceAttempt<AwaitInteractionStatus>> attempts = race(
        () -> complete(raced, Map.of("decision", "approved"), "completion-one-to-many-race-" + suffix, now + 5L)
            .record().status(),
        () -> org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
            .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
            .markTimedOut(raced.tenantId(), raced.interactionId(), raced.version(), now + 65_000L)
            .await().indefinitely().map(AwaitInteractionRecord::status).orElse(AwaitInteractionStatus.TIMED_OUT));
    assertTerminalRaceConverges(raced, attempts, Set.of(AwaitInteractionStatus.COMPLETED, AwaitInteractionStatus.TIMED_OUT));
  }

  private void manyToOneCompletesAfterEveryInput(boolean freshRuntime) {
    long now = System.currentTimeMillis();
    String suffix = freshRuntime ? "restart" : "uninterrupted";
    String tenant = "tenant-many-to-one-" + suffix;
    AwaitUnitStore workerAUnits = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX);
    AwaitInteractionStore workerAInteractions = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStore(dynamo, TABLE_PREFIX);
    AwaitUnitRecord unit = createUnit(workerAUnits, tenant, "execution-many-to-one-" + suffix,
        "unit-many-to-one-" + suffix, 2, "MANY_TO_ONE", now);
    AwaitInteractionRecord first = createInteraction(workerAInteractions, tenant, "many-to-one-a-" + suffix, now);
    AwaitInteractionRecord second = createInteraction(workerAInteractions, tenant, "many-to-one-b-" + suffix, now);
    workerAUnits.markDispatchComplete(tenant, unit.unitId(), 2, now + 1L).await().indefinitely().orElseThrow();
    workerAInteractions.complete(new AwaitCompletionCommand(
            first.tenantId(), first.interactionId(), first.correlationId(), "completion-many-to-one-a-" + suffix,
            Map.of("vote", "approved"), "provider", now + 2L))
        .await().indefinitely();
    AwaitUnitRecord held = workerAUnits.recordItemCompleted(tenant, unit.unitId(), "input:0", now + 3L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitUnitStatus.WAITING_EXTERNAL, held.status());

    AwaitInteractionStore finalCompleter = freshRuntime
        ? org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, TABLE_PREFIX)
        : workerAInteractions;
    AwaitUnitStore finalAggregator = freshRuntime
        ? org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX)
        : workerAUnits;
    finalCompleter.complete(new AwaitCompletionCommand(
            second.tenantId(), second.interactionId(), second.correlationId(), "completion-many-to-one-b-" + suffix,
            Map.of("vote", "approved"), "provider", now + 4L))
        .await().indefinitely();
    AwaitUnitRecord completed = finalAggregator.recordItemCompleted(tenant, unit.unitId(), "input:1", now + 5L)
        .await().indefinitely().orElseThrow();
    assertEquals("MANY_TO_ONE", completed.cardinality());
    assertEquals(AwaitUnitStatus.COMPLETED, completed.status());
    assertEquals(2, completed.completedItemCount());

    AwaitInteractionRecord raced = createInteraction(workerAInteractions, tenant, "many-to-one-cancel-" + suffix, now + 6L);
    List<RaceAttempt<AwaitInteractionStatus>> attempts = race(
        () -> complete(raced, Map.of("vote", "approved"), "completion-many-to-one-race-" + suffix, now + 7L)
            .record().status(),
        () -> org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
            .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
            .cancel(raced.tenantId(), raced.interactionId(), raced.version(), "cancelled", now + 7L)
            .await().indefinitely().map(AwaitInteractionRecord::status).orElse(AwaitInteractionStatus.CANCELLED));
    assertTerminalRaceConverges(raced, attempts, Set.of(AwaitInteractionStatus.COMPLETED, AwaitInteractionStatus.CANCELLED));
  }

  private void sequentialAwaitUnitsAdvanceInOrder(boolean freshRuntime) {
    long now = System.currentTimeMillis();
    String suffix = freshRuntime ? "restart" : "uninterrupted";
    String tenant = "tenant-sequential-" + suffix;
    AwaitUnitStore workerA = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX);
    AwaitUnitRecord first = createUnit(workerA, tenant, "execution-sequential-" + suffix,
        "unit-sequential-first-" + suffix, 2, "ONE_TO_ONE", now);
    workerA.markDispatchComplete(tenant, first.unitId(), 1, now + 1L).await().indefinitely().orElseThrow();
    AwaitUnitRecord firstCompleted = workerA.recordItemCompleted(tenant, first.unitId(), "first:0", now + 2L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitUnitStatus.COMPLETED, firstCompleted.status());

    AwaitUnitStore nextWorker = freshRuntime
        ? org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.unitStore(dynamo, TABLE_PREFIX)
        : workerA;
    AwaitUnitRecord restoredFirst = nextWorker.get(tenant, first.unitId()).await().indefinitely().orElseThrow();
    assertEquals(AwaitUnitStatus.COMPLETED, restoredFirst.status());
    AwaitUnitRecord second = createUnit(nextWorker, tenant, "execution-sequential-" + suffix,
        "unit-sequential-second-" + suffix, 4, "ONE_TO_ONE", now + 3L);
    nextWorker.markDispatchComplete(tenant, second.unitId(), 1, now + 4L).await().indefinitely().orElseThrow();
    AwaitUnitRecord secondCompleted = nextWorker.recordItemCompleted(tenant, second.unitId(), "second:0", now + 5L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitUnitStatus.COMPLETED, secondCompleted.status());
    assertTrue(restoredFirst.stepIndex() < secondCompleted.stepIndex());
  }

  private AwaitUnitRecord createUnit(
      AwaitUnitStore store,
      String tenant,
      String executionId,
      String unitId,
      int stepIndex,
      String cardinality,
      long now) {
    return store.createOrGet(new AwaitUnitCreateCommand(
            tenant, unitId, executionId, "AwaitDecision", stepIndex, cardinality, now, now / 1_000L + 3_600L))
        .await().indefinitely();
  }

  private void scalarCompletionPersistsAcrossAWorkerRestart() {
    long now = System.currentTimeMillis();
    AwaitInteractionStore workerA = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStore(dynamo, TABLE_PREFIX);
    var created = workerA.createOrGet(new AwaitCreateCommand(
            "tenant-scalar", "execution-scalar", "AwaitDecision", 2, String.class.getName(),
            "cause-scalar", "idempotency-scalar", "correlation-scalar", Map.of("request", "value"),
            null, null, "kafka", "unit-scalar", null, now, now + 60_000L, now / 1_000L + 3_600L))
        .await().indefinitely();
    assertFalse(created.duplicate());

    // New store instance: no worker A cache, live completion registry, or local observations.
    AwaitInteractionStore workerB = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, TABLE_PREFIX);
    var completed = workerB.complete(new AwaitCompletionCommand(
            "tenant-scalar", created.record().interactionId(), "correlation-scalar", "completion-scalar",
            Map.of("decision", "approved"), "provider", now + 1L))
        .await().indefinitely();
    assertFalse(completed.duplicate());
    assertEquals(AwaitInteractionStatus.COMPLETED, completed.record().status());

    AwaitInteractionRecord restored = workerB.get("tenant-scalar", created.record().interactionId())
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.COMPLETED, restored.status());
    assertEquals(Map.of("decision", "approved"), restored.responsePayload());

    var duplicate = workerA.complete(new AwaitCompletionCommand(
            "tenant-scalar", created.record().interactionId(), "correlation-scalar", "completion-scalar",
            Map.of("decision", "approved"), "provider", now + 2L))
        .await().indefinitely();
    assertTrue(duplicate.duplicate());
    assertEquals(restored.interactionId(), duplicate.record().interactionId());

    // A later conflicting physical completion is not allowed to replace the durable semantic
    // result selected by the first completion.
    var conflicting = workerA.complete(new AwaitCompletionCommand(
            "tenant-scalar", created.record().interactionId(), "correlation-scalar", "completion-conflict",
            Map.of("decision", "rejected"), "provider", now + 3L))
        .await().indefinitely();
    assertTrue(conflicting.duplicate());
    assertEquals(Map.of("decision", "approved"), conflicting.record().responsePayload());

  }

  private void duplicateCompletionHasOneDurableWinner() {
    concurrentDuplicateCompletionHasOneDurableWinner(newInteractionStore(), System.currentTimeMillis());
  }

  private void conflictingCompletionHasOneDurableWinner() {
    concurrentConflictingCompletionHasOneDurableWinner(newInteractionStore(), System.currentTimeMillis());
  }

  private void timeoutAndCompletionConverge() {
    concurrentTimeoutAndCompletionConverge(newInteractionStore(), System.currentTimeMillis());
  }

  private void cancellationAndCompletionConverge() {
    concurrentCancellationAndCompletionConverge(newInteractionStore(), System.currentTimeMillis());
  }

  private AwaitInteractionStore newInteractionStore() {
    return org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores.interactionStore(dynamo, TABLE_PREFIX);
  }

  private void concurrentDuplicateCompletionHasOneDurableWinner(AwaitInteractionStore creator, long now) {
    AwaitInteractionRecord interaction = createInteraction(creator, "tenant-race-duplicate", "duplicate", now);
    List<RaceAttempt<AwaitCompletionResult>> attempts = race(
        () -> complete(interaction, Map.of("decision", "approved"), "duplicate-a", now + 1L),
        () -> complete(interaction, Map.of("decision", "approved"), "duplicate-b", now + 1L));

    assertAcceptedExactlyOnce(interaction, attempts);
  }

  private void concurrentConflictingCompletionHasOneDurableWinner(AwaitInteractionStore creator, long now) {
    AwaitInteractionRecord interaction = createInteraction(creator, "tenant-race-conflict", "conflict", now);
    List<RaceAttempt<AwaitCompletionResult>> attempts = race(
        () -> complete(interaction, Map.of("decision", "approved"), "conflict-a", now + 1L),
        () -> complete(interaction, Map.of("decision", "rejected"), "conflict-b", now + 1L));

    assertAcceptedExactlyOnce(interaction, attempts);
  }

  private void concurrentTimeoutAndCompletionConverge(AwaitInteractionStore creator, long now) {
    AwaitInteractionRecord interaction = createInteraction(creator, "tenant-race-timeout", "timeout", now);
    List<RaceAttempt<AwaitInteractionStatus>> attempts = race(
        () -> complete(interaction, Map.of("decision", "approved"), "timeout-completion", now + 1L).record().status(),
        () -> org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
            .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
            .markTimedOut(interaction.tenantId(), interaction.interactionId(), interaction.version(), now + 61_000L)
            .await().indefinitely().map(AwaitInteractionRecord::status).orElse(AwaitInteractionStatus.TIMED_OUT));
    assertTerminalRaceConverges(interaction, attempts, Set.of(AwaitInteractionStatus.COMPLETED, AwaitInteractionStatus.TIMED_OUT));
  }

  private void concurrentCancellationAndCompletionConverge(AwaitInteractionStore creator, long now) {
    AwaitInteractionRecord interaction = createInteraction(creator, "tenant-race-cancel", "cancel", now);
    List<RaceAttempt<AwaitInteractionStatus>> attempts = race(
        () -> complete(interaction, Map.of("decision", "approved"), "cancel-completion", now + 1L).record().status(),
        () -> org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
            .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
            .cancel(interaction.tenantId(), interaction.interactionId(), interaction.version(), "cancelled", now + 1L)
            .await().indefinitely().map(AwaitInteractionRecord::status).orElse(AwaitInteractionStatus.CANCELLED));
    assertTerminalRaceConverges(interaction, attempts, Set.of(AwaitInteractionStatus.COMPLETED, AwaitInteractionStatus.CANCELLED));
  }

  private AwaitInteractionRecord createInteraction(
      AwaitInteractionStore creator,
      String tenantId,
      String key,
      long now) {
    return creator.createOrGet(new AwaitCreateCommand(
            tenantId, "execution-" + key, "AwaitDecision", 2, String.class.getName(),
            "cause-" + key, "idempotency-" + key, "correlation-" + key, Map.of("request", "value"),
            null, null, "kafka", "unit-" + key, null, now, now + 60_000L, now / 1_000L + 3_600L))
        .await().indefinitely().record();
  }

  private AwaitCompletionResult complete(
      AwaitInteractionRecord interaction,
      Map<String, String> response,
      String completionKey,
      long now) {
    AwaitInteractionStore completer = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, TABLE_PREFIX);
    return completer.complete(new AwaitCompletionCommand(
            interaction.tenantId(), interaction.interactionId(), interaction.correlationId(), completionKey,
            response, "provider", now))
        .await().indefinitely();
  }

  private static <T> List<RaceAttempt<T>> race(Callable<T> first, Callable<T> second) {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    try {
      Future<RaceAttempt<T>> firstAttempt = executor.submit(() -> invokeWhenReleased(first, ready, start));
      Future<RaceAttempt<T>> secondAttempt = executor.submit(() -> invokeWhenReleased(second, ready, start));
      if (!ready.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
        throw new IllegalStateException("Completion race contenders did not reach the start barrier");
      }
      start.countDown();
      return List.of(
          firstAttempt.get(10, java.util.concurrent.TimeUnit.SECONDS),
          secondAttempt.get(10, java.util.concurrent.TimeUnit.SECONDS));
    } catch (Exception exception) {
      throw new IllegalStateException("Failed executing deterministic completion race", exception);
    } finally {
      executor.shutdownNow();
    }
  }

  private static <T> RaceAttempt<T> invokeWhenReleased(
      Callable<T> call,
      CountDownLatch ready,
      CountDownLatch start) {
    try {
      ready.countDown();
      start.await();
      return new RaceAttempt<>(java.util.Optional.of(call.call()), java.util.Optional.empty());
    } catch (Throwable failure) {
      return new RaceAttempt<>(java.util.Optional.empty(), java.util.Optional.of(failure));
    }
  }

  private void assertAcceptedExactlyOnce(AwaitInteractionRecord interaction, List<RaceAttempt<AwaitCompletionResult>> attempts) {
    assertTrue(attempts.stream().allMatch(attempt -> attempt.failure().isEmpty()), () ->
        "Completion race failures: " + attempts.stream()
            .flatMap(attempt -> attempt.failure().stream())
            .map(failure -> failure.getClass().getSimpleName() + ": " + failure.getMessage())
            .toList());
    List<AwaitCompletionResult> results = attempts.stream().flatMap(attempt -> attempt.result().stream()).toList();
    assertEquals(1L, results.stream().filter(result -> !result.duplicate()).count());
    AwaitCompletionResult winner = results.stream().filter(result -> !result.duplicate()).findFirst().orElseThrow();
    AwaitInteractionRecord persisted = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
        .get(interaction.tenantId(), interaction.interactionId()).await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.COMPLETED, persisted.status());
    assertEquals(winner.record().responsePayload(), persisted.responsePayload());
  }

  private void assertTerminalRaceConverges(
      AwaitInteractionRecord interaction,
      List<RaceAttempt<AwaitInteractionStatus>> attempts,
      Set<AwaitInteractionStatus> allowedTerminalStates) {
    AwaitInteractionRecord persisted = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, TABLE_PREFIX)
        .get(interaction.tenantId(), interaction.interactionId()).await().indefinitely().orElseThrow();
    assertTrue(allowedTerminalStates.contains(persisted.status()));
    assertTrue(attempts.stream().flatMap(attempt -> attempt.result().stream())
        .allMatch(status -> status == persisted.status()));
  }

  private record RaceAttempt<T>(
      java.util.Optional<T> result,
      java.util.Optional<Throwable> failure) {
  }

  private void emptyItemizedUnitReleasesParentAfterAWorkerRestart() {
    ExecutionStateStore workerAStore = DynamoAwaitLifecycleTestStores.executionStoreForCreate(dynamo, TABLE_PREFIX);
    WorkDispatcher dispatcher = mock(WorkDispatcher.class);
    when(dispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
    long now = System.currentTimeMillis();
    long ttl = now / 1_000L + 3_600L;
    CreateExecutionResult parent = workerAStore.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-empty", "parent-empty", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI, now, ttl))
        .await().indefinitely();
    workerAStore.markWaitingExternal(
            "tenant-empty", parent.record().executionId(), parent.record().version(), "await", "unit-empty", 2, now)
        .await().indefinitely();
    ExecutionRecord<Object, Object> waitingParent = workerAStore.getExecution("tenant-empty", parent.record().executionId())
        .await().indefinitely().orElseThrow();
    AwaitUnitRecord emptyUnit = emptyUnit(waitingParent.executionId(), ttl);

    replaceScheduler();
    ExecutionStateStore workerBStore = DynamoAwaitLifecycleTestStores.executionStoreForPayloadMutation(dynamo, TABLE_PREFIX);
    flow(workerBStore, dispatcher).releaseParentIfReady(waitingParent, emptyUnit, 4, now)
        .await().indefinitely();

    ExecutionRecord<Object, Object> released = workerBStore.getExecution("tenant-empty", waitingParent.executionId())
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.QUEUED, released.status());
    assertEquals(4, released.currentStepIndex());
    ExecutionInputSnapshot payload = assertInstanceOf(ExecutionInputSnapshot.class, released.inputPayload());
    assertEquals(List.of(), payload.payload());
    verify(dispatcher).enqueueNow(new ExecutionWorkItem("tenant-empty", waitingParent.executionId()));

    // The admitted aggregate transition persists its next state before another fresh worker
    // restores it; no worker-local admission marker is required for replay.
    ExecutionRecord<Object, Object> terminal = workerBStore.markSucceeded(
            "tenant-empty", released.executionId(), released.version(), "empty-aggregate", List.of("published"), now + 1L)
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.SUCCEEDED, terminal.status());
    ExecutionStateStore workerCStore = DynamoAwaitLifecycleTestStores.executionStoreForPayloadMutation(dynamo, TABLE_PREFIX);
    ExecutionRecord<Object, Object> replayed = workerCStore.getExecution("tenant-empty", released.executionId())
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.SUCCEEDED, replayed.status());
    assertEquals(List.of("published"), replayed.resultPayload());
  }

  private void terminalCleanupLeavesNoDurableAwaitWork() {
    long now = System.currentTimeMillis();
    long ttl = now / 1_000L + 3_600L;
    String tenantId = "tenant-terminal-cleanup";
    ExecutionStateStore executionStore = DynamoAwaitLifecycleTestStores.executionStoreForCreate(dynamo, TABLE_PREFIX);
    WorkDispatcher dispatcher = mock(WorkDispatcher.class);
    when(dispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
    CreateExecutionResult created = executionStore.createOrGetExecution(new ExecutionCreateCommand(
            tenantId, "terminal-cleanup-parent", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI, now, ttl))
        .await().indefinitely();
    executionStore.markWaitingExternal(
            tenantId, created.record().executionId(), created.record().version(), "await", "unit-terminal-cleanup", 2, now)
        .await().indefinitely();
    ExecutionRecord<Object, Object> waiting = executionStore
        .getExecution(tenantId, created.record().executionId()).await().indefinitely().orElseThrow();

    AwaitUnitStore unitStore = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .unitStore(dynamo, TABLE_PREFIX);
    AwaitUnitRecord createdUnit = unitStore.createOrGet(new AwaitUnitCreateCommand(
            tenantId, "unit-terminal-cleanup", waiting.executionId(), "AwaitPaymentProvider", 2,
            "ONE_TO_ONE", now, ttl))
        .await().indefinitely();
    AwaitUnitRecord completedUnit = unitStore.markDispatchComplete(
            tenantId, createdUnit.unitId(), 0, now + 1L)
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitUnitStatus.COMPLETED, completedUnit.status());

    replaceScheduler();
    ExecutionStateStore freshExecutionStore = DynamoAwaitLifecycleTestStores
        .executionStoreForPayloadMutation(dynamo, TABLE_PREFIX);
    flow(freshExecutionStore, dispatcher).releaseParentIfReady(waiting, completedUnit, 4, now + 2L)
        .await().indefinitely();
    ExecutionRecord<Object, Object> released = freshExecutionStore
        .getExecution(tenantId, waiting.executionId()).await().indefinitely().orElseThrow();
    ExecutionRecord<Object, Object> terminal = freshExecutionStore.markSucceeded(
            tenantId, released.executionId(), released.version(), "terminal-cleanup", List.of("published"), now + 3L)
        .await().indefinitely().orElseThrow();

    AwaitUnitStore freshUnits = org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores
        .unitStore(dynamo, TABLE_PREFIX);
    assertEquals(ExecutionStatus.SUCCEEDED, terminal.status());
    assertEquals(AwaitUnitStatus.COMPLETED, freshUnits.get(tenantId, completedUnit.unitId())
        .await().indefinitely().orElseThrow().status());
    assertFalse(hasInteractionForUnit(tenantId, completedUnit.unitId()),
        "empty itemized completion must leave no pending interactions");
    assertEquals(List.of("published"), freshExecutionStore
        .getExecution(tenantId, terminal.executionId()).await().indefinitely().orElseThrow().resultPayload());
  }

  private boolean hasInteractionForUnit(String tenantId, String unitId) {
    Map<String, AttributeValue> startKey = Map.of();
    do {
      ScanRequest.Builder request = ScanRequest.builder().tableName(TABLE_PREFIX + "_interaction");
      if (!startKey.isEmpty()) {
        request.exclusiveStartKey(startKey);
      }
      ScanResponse page = dynamo.scan(request.build());
      boolean found = page.items().stream().anyMatch(item -> tenantId.equals(attribute(item, "tenant_id"))
          && unitId.equals(attribute(item, "unit_id")));
      if (found) {
        return true;
      }
      startKey = page.lastEvaluatedKey() == null ? Map.of() : page.lastEvaluatedKey();
    } while (!startKey.isEmpty());
    return false;
  }

  private static String attribute(Map<String, AttributeValue> item, String name) {
    AttributeValue value = item.get(name);
    return value == null ? "" : value.s();
  }

  private void finalChildAfterRestartReleasesParentFromDurableChildren() {
    ExecutionStateStore workerAStore = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
    WorkDispatcher dispatcher = mock(WorkDispatcher.class);
    when(dispatcher.enqueueNow(any())).thenReturn(Uni.createFrom().voidItem());
    long now = System.currentTimeMillis();
    long ttl = now / 1_000L + 3_600L;
    CreateExecutionResult parent = workerAStore.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-restart",
            "parent-key",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            ttl))
        .await().indefinitely();
    workerAStore.markWaitingExternal(
            "tenant-restart", parent.record().executionId(), parent.record().version(),
            "await", "unit-1", 2, now)
        .await().indefinitely();
    ExecutionRecord<Object, Object> waitingParent = workerAStore.getExecution("tenant-restart", parent.record().executionId())
        .await().indefinitely().orElseThrow();
    AwaitUnitRecord unit = unit(waitingParent.executionId(), ttl);

    CreateExecutionResult firstChild = workerAStore.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-restart",
            ItemContinuationKey.from(waitingParent, unit, 0).childExecutionKey(),
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "first"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            ttl))
        .await().indefinitely();
    workerAStore.markSucceeded(
            "tenant-restart", firstChild.record().executionId(), firstChild.record().version(),
            "worker-a", List.of("out-0"), now)
        .await().indefinitely();

    // Worker B has a new coordinator, scheduler, claim set, and store instance.  Only Dynamo
    // contains evidence that item zero has already succeeded.
    ExecutionStateStore workerBStore = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
    replaceScheduler();
    ItemizedAwaitContinuationFlow workerB = new ItemizedAwaitContinuationFlow(
        workerBStore,
        dispatcher,
        mock(AwaitCoordinator.class),
        new TransitionWorkerExecutor(null, new PipelineInvocationRuntime()),
        scheduler,
        () -> Duration.ofMillis(10),
        () -> new SegmentBoundaryLedger(new InMemoryControlPlaneJournal()),
        ignored -> {
        },
        new AwaitContinuationPlanner(),
        new ItemContinuationClaims());

    workerB.captureOutput(
            interaction(waitingParent.executionId(), 1),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second"),
            List.of("out-1"),
            now)
        .await().indefinitely();

    ExecutionRecord<Object, Object> resumed = workerBStore.getExecution("tenant-restart", waitingParent.executionId())
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.QUEUED, resumed.status());
    assertEquals(4, resumed.currentStepIndex());
    ExecutionInputSnapshot payload = assertInstanceOf(ExecutionInputSnapshot.class, resumed.inputPayload());
    assertEquals(List.of("out-0", "out-1"), payload.payload());
    verify(dispatcher).enqueueNow(new ExecutionWorkItem("tenant-restart", waitingParent.executionId()));

    // Simulate a duplicate physical completion on another fresh worker.  The durable parent state,
    // rather than either worker's local claim set, decides whether a semantic release is still due.
    ItemizedAwaitContinuationFlow workerC = flow(workerBStore, dispatcher);
    workerC.captureOutput(
            interaction(waitingParent.executionId(), 1),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "second"),
            List.of("out-1"),
            now + 1)
        .await().indefinitely();

    ExecutionRecord<Object, Object> afterDuplicate = workerBStore.getExecution("tenant-restart", waitingParent.executionId())
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.QUEUED, afterDuplicate.status());
    assertEquals(4, afterDuplicate.currentStepIndex());
    assertEquals(payload, afterDuplicate.inputPayload());
    // Physical enqueue attempts are deliberately at-least-once.  The duplicate must not advance
    // the durable parent beyond the one semantic continuation already accepted above.
    verify(dispatcher, times(2)).enqueueNow(new ExecutionWorkItem("tenant-restart", waitingParent.executionId()));

  }

  private void recoversItemContinuationAfterWorkerDiesWithAnAdmittedTransition() {
    ExecutionStateStore workerAStore = DynamoAwaitLifecycleTestStores.executionStoreForCreate(dynamo, TABLE_PREFIX);
    long now = System.currentTimeMillis();
    long ttl = now / 1_000L + 3_600L;
    CreateExecutionResult parent = workerAStore.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-admission", "parent-admission", new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI, now, ttl))
        .await().indefinitely();
    workerAStore.markWaitingExternal(
            "tenant-admission", parent.record().executionId(), parent.record().version(), "await", "unit-admission", 2, now)
        .await().indefinitely();
    ExecutionRecord<Object, Object> waitingParent = workerAStore.getExecution("tenant-admission", parent.record().executionId())
        .await().indefinitely().orElseThrow();
    AwaitUnitRecord unit = new AwaitUnitRecord(
        "tenant-admission", "unit-admission", waitingParent.executionId(), "AwaitPaymentProvider", 2, "ONE_TO_ONE", 1L,
        AwaitUnitStatus.COMPLETED, null, 1, 1, Set.of("item:0"), true, 1L, 2L, ttl);
    AwaitInteractionRecord interaction = interaction(waitingParent.executionId(), 0, "tenant-admission");
    AwaitCoordinator coordinator = mock(AwaitCoordinator.class);
    when(coordinator.findByUnit("tenant-admission", "unit-admission"))
        .thenReturn(Uni.createFrom().item(List.of(interaction)));
    CountDownLatch admitted = new CountDownLatch(1);
    AwaitItemContinuationHandler blockedHandler = new AwaitItemContinuationHandler() {
      @Override
      public Uni<Void> continueAwaitItem(AwaitInteractionRecord ignored, AwaitUnitRecord ignoredUnit, int nextStep,
          java.util.Optional<ExecutionRecord<Object, Object>> ignoredParent, long ignoredNow) {
        admitted.countDown();
        return Uni.createFrom().nothing();
      }

      @Override
      public Uni<Void> releaseAwaitParentIfReady(ExecutionRecord<Object, Object> ignoredParent, AwaitUnitRecord ignoredUnit,
          int nextStep, long ignoredNow) {
        return Uni.createFrom().voidItem();
      }
    };
    replaceScheduler();
    flow(workerAStore, mock(WorkDispatcher.class), coordinator).afterParentWaiting(
            waitingParent, unit, 2, blockedHandler, now)
        .await().indefinitely();
    try {
      assertTrue(admitted.await(10, java.util.concurrent.TimeUnit.SECONDS));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for item continuation admission", interrupted);
    }

    // Worker A disappears after it has admitted the transition.  Worker B has no A-local claim or permit.
    scheduler.shutdownNow();
    AtomicInteger recoveredCalls = new AtomicInteger();
    CountDownLatch recovered = new CountDownLatch(1);
    AwaitItemContinuationHandler recoveredHandler = new AwaitItemContinuationHandler() {
      @Override
      public Uni<Void> continueAwaitItem(AwaitInteractionRecord ignored, AwaitUnitRecord ignoredUnit, int nextStep,
          java.util.Optional<ExecutionRecord<Object, Object>> restoredParent, long ignoredNow) {
        assertEquals(waitingParent.executionId(), restoredParent.orElseThrow().executionId());
        recoveredCalls.incrementAndGet();
        recovered.countDown();
        return Uni.createFrom().voidItem();
      }

      @Override
      public Uni<Void> releaseAwaitParentIfReady(ExecutionRecord<Object, Object> ignoredParent, AwaitUnitRecord ignoredUnit,
          int nextStep, long ignoredNow) {
        return Uni.createFrom().voidItem();
      }
    };
    replaceScheduler();
    flow(DynamoAwaitLifecycleTestStores.executionStoreForExistingState(dynamo, TABLE_PREFIX), mock(WorkDispatcher.class), coordinator)
        .afterParentWaiting(waitingParent, unit, 2, recoveredHandler, now + 1L)
        .await().indefinitely();
    try {
      assertTrue(recovered.await(10, java.util.concurrent.TimeUnit.SECONDS));
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted waiting for recovered item continuation", interrupted);
    }
    assertEquals(1, recoveredCalls.get());
  }

  private void partialDurableChildrenNeverReleaseParentFromLocalClaims() {
    ExecutionStateStore store = DynamoAwaitLifecycleTestStores.executionStore(dynamo, TABLE_PREFIX);
    WorkDispatcher dispatcher = mock(WorkDispatcher.class);
    long now = System.currentTimeMillis();
    long ttl = now / 1_000L + 3_600L;
    CreateExecutionResult parent = store.createOrGetExecution(new ExecutionCreateCommand(
            "tenant-premature",
            "parent-key",
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "input"),
            ExecutionResultShape.MATERIALIZED_MULTI,
            now,
            ttl))
        .await().indefinitely();
    store.markWaitingExternal(
            "tenant-premature", parent.record().executionId(), parent.record().version(),
            "await", "unit-1", 2, now)
        .await().indefinitely();
    ExecutionRecord<Object, Object> waitingParent = store.getExecution("tenant-premature", parent.record().executionId())
        .await().indefinitely().orElseThrow();
    AwaitUnitRecord unit = unit(waitingParent.executionId(), ttl, "tenant-premature");

    replaceScheduler();
    ItemizedAwaitContinuationFlow flow = flow(store, dispatcher);
    flow.captureOutput(
            interaction(waitingParent.executionId(), 0, "tenant-premature"),
            unit,
            4,
            new ExecutionInputSnapshot(ExecutionInputShape.UNI, "first"),
            List.of("out-0"),
            now)
        .await().indefinitely();

    ExecutionRecord<Object, Object> held = store.getExecution("tenant-premature", waitingParent.executionId())
        .await().indefinitely().orElseThrow();
    assertEquals(ExecutionStatus.WAITING_EXTERNAL, held.status());
    assertEquals("unit-1", held.awaitUnitId());
    verify(dispatcher, org.mockito.Mockito.never()).enqueueNow(any());
    assertTrue(store.getExecutionByKey(
            "tenant-premature", ItemContinuationKey.from(waitingParent, unit, 1).childExecutionKey())
        .await().indefinitely().isEmpty());
  }

  private void createTable(String name, String hash, String range) {
    CreateTableRequest.Builder table = CreateTableRequest.builder()
        .tableName(name)
        .attributeDefinitions(range == null
            ? List.of(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build())
            : List.of(
                AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build()))
        .keySchema(range == null
            ? List.of(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build())
            : List.of(
                KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build()))
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build());
    dynamo.createTable(table.build());
    dynamo.waiter().waitUntilTableExists(request -> request.tableName(name));
  }

  private ItemizedAwaitContinuationFlow flow(ExecutionStateStore store, WorkDispatcher dispatcher) {
    return flow(store, dispatcher, mock(AwaitCoordinator.class));
  }

  private ItemizedAwaitContinuationFlow flow(ExecutionStateStore store, WorkDispatcher dispatcher, AwaitCoordinator coordinator) {
    return new ItemizedAwaitContinuationFlow(
        store,
        dispatcher,
        coordinator,
        new TransitionWorkerExecutor(null, new PipelineInvocationRuntime()),
        scheduler,
        () -> Duration.ofMillis(10),
        () -> new SegmentBoundaryLedger(new InMemoryControlPlaneJournal()),
        ignored -> {
        },
        new AwaitContinuationPlanner(),
        new ItemContinuationClaims());
  }

  private static AwaitUnitRecord unit(String executionId, long ttl) {
    return unit(executionId, ttl, "tenant-restart");
  }

  private static AwaitUnitRecord unit(String executionId, long ttl, String tenantId) {
    return new AwaitUnitRecord(
        tenantId, "unit-1", executionId, "AwaitPaymentProvider", 2, "ONE_TO_ONE", 1L,
        AwaitUnitStatus.COMPLETED, null, 2, 2, Set.of("item:0", "item:1"), true, 1L, 2L, ttl);
  }

  private static AwaitUnitRecord emptyUnit(String executionId, long ttl) {
    return new AwaitUnitRecord(
        "tenant-empty", "unit-empty", executionId, "AwaitPaymentProvider", 2, "ONE_TO_ONE", 1L,
        AwaitUnitStatus.COMPLETED, null, 0, 0, Set.of(), true, 1L, 2L, ttl);
  }

  private static AwaitInteractionRecord interaction(String executionId, int index) {
    return interaction(executionId, index, "tenant-restart");
  }

  private static AwaitInteractionRecord interaction(String executionId, int index, String tenantId) {
    return new AwaitInteractionRecord(
        tenantId, executionId, "AwaitPaymentProvider", 2, String.class.getName(),
        "interaction-" + index, "correlation-" + index, "cause", "idempotency-" + index, 1L,
        AwaitInteractionStatus.COMPLETED, Map.of("value", "request"), Map.of("value", "response"),
        "unit-1", index, "user", null, null, "kafka", Map.of(), 10_000L, 1L, 2L, 9_999_999L);
  }
}
