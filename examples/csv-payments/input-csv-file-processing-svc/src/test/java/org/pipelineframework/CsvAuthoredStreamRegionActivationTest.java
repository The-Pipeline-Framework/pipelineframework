package org.pipelineframework;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.UnsatisfiedResolutionException;
import jakarta.enterprise.util.TypeLiteral;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitContinuationStatus;
import org.pipelineframework.awaitable.AwaitStepDescriptorFactory;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.awaitable.AwaitResumeTokenService;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitTransportAdapter;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitInteractionStore;
import org.pipelineframework.awaitable.store.InMemoryAwaitUnitStore;
import org.pipelineframework.csv.domain.CsvPaymentsInputFile;
import org.pipelineframework.csv.common.mapper.PaymentRecordRepresentationMapper;
import org.pipelineframework.csv.service.ProcessCsvPaymentsInputService;
import org.pipelineframework.csv.service.ProcessCsvPaymentsInputServicePipelineFacade;
import org.pipelineframework.csv.service.pipeline.ProcessCsvPaymentsInputStreamRegionContinuation;
import org.pipelineframework.orchestrator.ControlPlaneAdmissionDecision;
import org.pipelineframework.orchestrator.ExecutionCreateCommand;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionStatus;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.InMemoryControlPlaneTransactionLock;
import org.pipelineframework.orchestrator.InMemoryExecutionStateStore;
import org.pipelineframework.orchestrator.JsonTransitionPayloadCodec;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.WorkDispatcher;
import org.pipelineframework.orchestrator.controlplane.InMemoryControlPlaneJournal;
import org.pipelineframework.orchestrator.controlplane.SegmentBoundaryLedger;
import org.pipelineframework.orchestrator.stream.StreamRegionPageCommit;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.orchestrator.stream.StreamRegionStatus;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.invocation.PipelineInvocationRuntime;
import org.pipelineframework.stream.StreamRegionContinuationRegistry;

/**
 * Authored CSV activation smoke test.  It deliberately starts with the generated continuation
 * rather than the legacy producer {@code Multi}, so the following additions can exercise the
 * durable coordinator route without making OpenCSV paging a test double.
 */
class CsvAuthoredStreamRegionActivationTest {

  @Test
  void generatedCsvContinuationReadsOneBoundedOpenCsvPage() {
    ProcessCsvPaymentsInputStreamRegionContinuation continuation = generatedContinuation();
    Path csv = sampleCsv();
    CsvPaymentsInputFile source = new CsvPaymentsInputFile(csv, csv.getParent());

    ProcessCsvPaymentsInputStreamRegionContinuation.Page page = continuation
        .applyOneToOne(continuation.inputFor(source, org.pipelineframework.stream.OpaqueSourceCheckpoint.initial(), 2))
        .await().indefinitely();

    assertEquals(2, page.items().size());
    assertTrue(page.nextCheckpoint().value().isPresent());
  }

  @Test
  void authoredCsvProducerActivatesARegionBeforeMultiThenCompletesOneBoundedPage() {
    ProcessCsvPaymentsInputStreamRegionContinuation continuation = generatedContinuation();
    StreamRegionContinuationRegistry continuationRegistry = registry(continuation);
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(OrchestratorMode.QUEUE_ASYNC);
    when(config.leaseMs()).thenReturn(60_000L);
    when(config.sweepLimit()).thenReturn(10);

    InMemoryControlPlaneTransactionLock transactionLock = new InMemoryControlPlaneTransactionLock();
    InMemoryExecutionStateStore executionStore = new InMemoryExecutionStateStore(transactionLock);
    InMemoryAwaitInteractionStore interactionStore = new InMemoryAwaitInteractionStore(transactionLock, executionStore);
    RecordingAdapter adapter = new RecordingAdapter();
    AwaitCoordinator awaitCoordinator = coordinator(config, interactionStore, new InMemoryAwaitUnitStore(), adapter);
    RecordingDispatcher dispatcher = new RecordingDispatcher();
    CsvPaymentsInputFile source = source();
    String executionId = executionStore.createOrGetExecution(new ExecutionCreateCommand(
        "csv-activation", "small-authored-csv", new ExecutionInputSnapshot(ExecutionInputShape.UNI, source),
        ExecutionResultShape.SINGLE, 60_000L, Long.MAX_VALUE)).await().indefinitely().record().executionId();

    QueueAsyncSegmentPipeline activation = new QueueAsyncSegmentPipeline(
        config,
        executionStore,
        dispatcher,
        awaitCoordinator,
        continuationRegistry,
        new TransitionWorkerExecutor(config, new PipelineInvocationRuntime()),
        ignored -> ControlPlaneAdmissionDecision.allow(),
        JsonTransitionPayloadCodec::new,
        () -> new SegmentBoundaryLedger(new InMemoryControlPlaneJournal()),
        () -> java.time.Duration.ofMillis(25),
        () -> 5,
        mock(SegmentCommitEffects.class),
        "csv-activation-worker");

    activation.process(new ExecutionWorkItem("csv-activation", executionId),
        ignored -> Uni.createFrom().failure(new AssertionError("legacy producer Multi must not execute")),
        AwaitContinuations.NOOP_ITEM_CONTINUATION_HANDLER).await().indefinitely();

    ExecutionWorkItem regionWork = dispatcher.onlyStreamRegionWork();
    StreamRegionRecord activated = executionStore.getStreamRegion("csv-activation", executionId,
        regionWork.streamRegionId()).await().indefinitely().orElseThrow();
    assertEquals(0L, activated.nextLogicalOrdinal());
    assertEquals(0, activated.outstandingCredits());
    assertEquals(ExecutionStatus.WAITING_EXTERNAL,
        executionStore.getExecution("csv-activation", executionId).await().indefinitely().orElseThrow().status());
    assertTrue(continuationRegistry.findForProducerStep(0).isPresent());

    StreamRegionFlow regionFlow = new StreamRegionFlow(executionStore, awaitCoordinator, continuationRegistry,
        new TransitionWorkerExecutor(config, new PipelineInvocationRuntime()), new JsonTransitionPayloadCodec(), config);
    regionFlow.process(regionWork, worker(continuation)).await().indefinitely();

    StreamRegionRecord sealed = executionStore.getStreamRegion("csv-activation", executionId,
        regionWork.streamRegionId()).await().indefinitely().orElseThrow();
    assertEquals(StreamRegionStatus.SOURCE_SEALED, sealed.status());
    assertEquals(2L, sealed.nextLogicalOrdinal());
    assertEquals(2, sealed.outstandingCredits());

    String firstId = StreamRegionPageCommit.interactionId(activated, 0);
    String secondId = StreamRegionPageCommit.interactionId(activated, 1);
    assertEquals(List.of(firstId, secondId), adapter.dispatchedInteractionIds());
    completeAndApply(config, awaitCoordinator, executionStore, dispatcher, executionId, secondId, "completion-two");
    assertTrue(dispatcher.hasAwaitInteraction(secondId));
    assertEquals(1, region(executionStore, executionId, regionWork).outstandingCredits());
    assertEquals(StreamRegionStatus.SOURCE_SEALED, region(executionStore, executionId, regionWork).status());
    completeAndApply(config, awaitCoordinator, executionStore, dispatcher, executionId, firstId, "completion-one");

    StreamRegionRecord completed = region(executionStore, executionId, regionWork);
    assertEquals(StreamRegionStatus.COMPLETED, completed.status());
    assertEquals(0, completed.outstandingCredits());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactionStore.get("csv-activation", firstId).await().indefinitely().orElseThrow().continuationStatus());
    assertEquals(AwaitContinuationStatus.APPLIED,
        interactionStore.get("csv-activation", secondId).await().indefinitely().orElseThrow().continuationStatus());
    assertEquals(ExecutionStatus.SUCCEEDED,
        executionStore.getExecution("csv-activation", executionId).await().indefinitely().orElseThrow().status());
  }

  private static StreamRegionRecord region(
      ExecutionStateStore store,
      String executionId,
      ExecutionWorkItem work
  ) {
    return store.getStreamRegion("csv-activation", executionId, work.streamRegionId())
        .await().indefinitely().orElseThrow();
  }

  private static void completeAndApply(
      PipelineOrchestratorConfig config,
      AwaitCoordinator coordinator,
      ExecutionStateStore executionStore,
      RecordingDispatcher dispatcher,
      String executionId,
      String interactionId,
      String completionKey
  ) {
    SegmentBoundaryLedger ledger = new SegmentBoundaryLedger(new InMemoryControlPlaneJournal());
    AwaitBoundaryAdmission admission = new AwaitBoundaryAdmission(
        config, new ExecutionInputPolicy(), ignored -> ControlPlaneAdmissionDecision.allow(), coordinator, null,
        () -> "csv-payments", () -> "1", () -> ledger, mock(AwaitContinuations.class), dispatcher);
    org.pipelineframework.csv.grpc.PipelineTypes.PaymentStatus response =
        org.pipelineframework.csv.grpc.PipelineTypes.PaymentStatus.newBuilder()
            .setApproved(org.pipelineframework.csv.grpc.PipelineTypes.ApprovedPaymentStatus.newBuilder()
                .setReference(interactionId)
                .setStatus("approved")
                .build())
            .build();
    admission.complete(new AwaitCompletionCommand("csv-activation", interactionId, null, completionKey, response, "test",
        System.currentTimeMillis()), scalarSuffix()).await().indefinitely();
    new DurableAwaitItemContinuationFlow(coordinator, dispatcher, executionStore)
        .process(ExecutionWorkItem.awaitContinuation("csv-activation", executionId, interactionId), scalarSuffix())
        .await().indefinitely();
  }

  private static AwaitItemContinuationHandler scalarSuffix() {
    return new AwaitItemContinuationHandler() {
      @Override
      public Uni<Void> continueAwaitItem(
          AwaitInteractionRecord interaction,
          AwaitUnitRecord unit,
          int nextStepIndex,
          Optional<ExecutionRecord<Object, Object>> parent,
          long nowEpochMs
      ) {
        return Uni.createFrom().failure(new AssertionError("legacy itemized continuation must not run"));
      }

      @Override
      public Uni<Object> continueDurableAwaitItem(
          AwaitInteractionRecord interaction,
          AwaitUnitRecord unit,
          int nextStepIndex,
          Optional<ExecutionRecord<Object, Object>> parent,
          long nowEpochMs
      ) {
        return Uni.createFrom().item(interaction.responsePayload());
      }

      @Override
      public Uni<Void> releaseAwaitParentIfReady(
          ExecutionRecord<Object, Object> parent,
          AwaitUnitRecord unit,
          int nextStepIndex,
          long nowEpochMs
      ) {
        return Uni.createFrom().failure(new AssertionError("legacy itemized parent release must not run"));
      }
    };
  }

  private static AwaitCoordinator coordinator(
      PipelineOrchestratorConfig config,
      AwaitInteractionStore interactionStore,
      AwaitUnitStore unitStore,
      AwaitTransportAdapter<?> adapter
  ) {
    AwaitCoordinator coordinator = new AwaitCoordinator();
    set(coordinator, "interactionStores", new TestInstance<>(List.of(interactionStore)));
    set(coordinator, "unitStores", new TestInstance<>(List.of(unitStore)));
    set(coordinator, "adapters", new TestInstance<>(List.of(adapter)));
    set(coordinator, "orchestratorConfig", config);
    set(coordinator, "resumeTokenService", construct(AwaitResumeTokenService.class,
        new Class<?>[] {String.class}, "csv-authored-stream-test"));
    set(coordinator, "descriptorFactory", new AwaitStepDescriptorFactory());
    return coordinator;
  }

  private static CsvPaymentsInputFile source() {
    Path csv = sampleCsv();
    return new CsvPaymentsInputFile(csv, csv.getParent());
  }

  private static ProcessCsvPaymentsInputStreamRegionContinuation generatedContinuation() {
    ProcessCsvPaymentsInputServicePipelineFacade facade = new ProcessCsvPaymentsInputServicePipelineFacade();
    set(facade, "delegate", new ProcessCsvPaymentsInputService());
    set(facade, "mapper", new PaymentRecordRepresentationMapper());
    ProcessCsvPaymentsInputStreamRegionContinuation continuation =
        new ProcessCsvPaymentsInputStreamRegionContinuation();
    set(continuation, "facade", facade);
    set(continuation, "descriptorFactory", new AwaitStepDescriptorFactory());
    return continuation;
  }

  private static StreamRegionContinuationRegistry registry(
      ProcessCsvPaymentsInputStreamRegionContinuation continuation
  ) {
    StreamRegionContinuationRegistry registry = new StreamRegionContinuationRegistry();
    set(registry, "continuations", new TestInstance<>(List.of(continuation)));
    return registry;
  }

  private static PipelineTransitionWorker worker(
      ProcessCsvPaymentsInputStreamRegionContinuation continuation
  ) {
    JsonTransitionPayloadCodec codec = new JsonTransitionPayloadCodec();
    return envelope -> {
      Object input = envelope.toCommand(codec).inputPayload();
      ProcessCsvPaymentsInputStreamRegionContinuation.Input pageInput =
          (ProcessCsvPaymentsInputStreamRegionContinuation.Input) ((ExecutionInputSnapshot) input).payload();
      return continuation.applyOneToOne(pageInput)
          .map(page -> TransitionResultEnvelope.completed(codec, List.of(page)));
    };
  }

  private static Path sampleCsv() {
    return Path.of("src", "test", "resources", "stream-activation.csv")
        .toAbsolutePath().normalize();
  }

  private static void set(Object target, String fieldName, Object value) {
    try {
      Field field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot set " + fieldName, failure);
    }
  }

  private static <T> T construct(Class<T> type, Class<?>[] parameterTypes, Object... arguments) {
    try {
      var constructor = type.getDeclaredConstructor(parameterTypes);
      constructor.setAccessible(true);
      return constructor.newInstance(arguments);
    } catch (ReflectiveOperationException failure) {
      throw new IllegalStateException("Cannot construct " + type.getName(), failure);
    }
  }

  private static final class RecordingAdapter implements AwaitTransportAdapter<Object> {
    private final List<String> interactionIds = new ArrayList<>();

    @Override
    public String type() {
      return "kafka";
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

    ExecutionWorkItem onlyStreamRegionWork() {
      return items.stream().filter(ExecutionWorkItem::streamRegion).findFirst().orElseThrow();
    }

    boolean hasAwaitInteraction(String interactionId) {
      return items.stream().anyMatch(item -> item.awaitContinuation()
          && interactionId.equals(item.awaitInteractionId()));
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
