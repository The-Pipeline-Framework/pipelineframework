package org.pipelineframework;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.awaitable.AwaitCoordinator;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitUnitRecord;
import org.pipelineframework.orchestrator.ExecutionInputShape;
import org.pipelineframework.orchestrator.ExecutionInputSnapshot;
import org.pipelineframework.orchestrator.ExecutionRecord;
import org.pipelineframework.orchestrator.ExecutionResultShape;
import org.pipelineframework.orchestrator.ExecutionStateStore;
import org.pipelineframework.orchestrator.ExecutionWorkItem;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.PipelineTransitionWorker;
import org.pipelineframework.orchestrator.TransitionCommandEnvelope;
import org.pipelineframework.orchestrator.TransitionPayloadCodec;
import org.pipelineframework.orchestrator.TransitionResultEnvelope;
import org.pipelineframework.orchestrator.TransitionWorkerCommand;
import org.pipelineframework.orchestrator.TransitionWorkerExecutor;
import org.pipelineframework.orchestrator.TransitionWorkerOutcome;
import org.pipelineframework.orchestrator.stream.StreamRegionPageCommit;
import org.pipelineframework.orchestrator.stream.StreamRegionRecord;
import org.pipelineframework.stream.StreamRegionAwaitBinding;
import org.pipelineframework.stream.StreamRegionContinuation;
import org.pipelineframework.stream.StreamRegionContinuationRegistry;
import org.pipelineframework.stream.StreamRegionContinuationResult;

/** Coordinator-owned bridge from a claimed producer region to one ordinary worker transition. */
final class StreamRegionFlow {

  /**
   * Pinned Dynamo physical transaction layout: one region write plus three writes per item must
   * remain below Dynamo's 100-action bound. It is not a semantic stream-window constant; each
   * region persists its own independently chosen credit window.
   */
  static final int DYNAMO_PAGE_ITEM_LAYOUT_LIMIT = 32;

  private final ExecutionStateStore executionStateStore;
  private final AwaitCoordinator awaitCoordinator;
  private final StreamRegionContinuationRegistry continuations;
  private final TransitionWorkerExecutor transitionWorkerExecutor;
  private final TransitionPayloadCodec payloadCodec;
  private final PipelineOrchestratorConfig config;

  StreamRegionFlow(
      ExecutionStateStore executionStateStore,
      AwaitCoordinator awaitCoordinator,
      StreamRegionContinuationRegistry continuations,
      TransitionWorkerExecutor transitionWorkerExecutor,
      TransitionPayloadCodec payloadCodec,
      PipelineOrchestratorConfig config) {
    this.executionStateStore = executionStateStore;
    this.awaitCoordinator = awaitCoordinator;
    this.continuations = continuations;
    this.transitionWorkerExecutor = transitionWorkerExecutor;
    this.payloadCodec = payloadCodec;
    this.config = config;
  }

  Uni<Void> process(ExecutionWorkItem workItem, PipelineTransitionWorker worker) {
    long now = System.currentTimeMillis();
    String owner = UUID.randomUUID().toString();
    return executionStateStore.claimStreamRegion(
            workItem.tenantId(), workItem.executionId(), workItem.streamRegionId(), owner, now, config.leaseMs())
        .onItem().transformToUni(claimed -> claimed
            .map(region -> processClaimed(workItem, worker, region, now))
            .orElseGet(() -> Uni.createFrom().voidItem()));
  }

  private Uni<Void> processClaimed(
      ExecutionWorkItem workItem,
      PipelineTransitionWorker worker,
      StreamRegionRecord region,
      long nowEpochMs) {
    if (region.sourceSealed() || region.availableCredits() == 0) {
      return Uni.createFrom().voidItem();
    }
    StreamRegionContinuation continuation = continuations.find(region.source())
        .orElseThrow(() -> new IllegalStateException("No generated stream continuation matches " + region.source()));
    StreamRegionAwaitBinding binding = continuation.awaitBinding();
    return executionStateStore.getExecution(workItem.tenantId(), workItem.executionId())
        .onItem().transformToUni(execution -> execution
            .map(record -> materializeClaimedPage(worker, region, record, continuation, binding, nowEpochMs))
            .orElseGet(() -> Uni.createFrom().failure(new IllegalStateException(
                "Owning execution is missing for stream region " + region.regionId()))));
  }

  private Uni<Void> materializeClaimedPage(
      PipelineTransitionWorker worker,
      StreamRegionRecord region,
      ExecutionRecord<Object, Object> execution,
      StreamRegionContinuation continuation,
      StreamRegionAwaitBinding binding,
      long nowEpochMs) {
    int limit = Math.min(region.availableCredits(), DYNAMO_PAGE_ITEM_LAYOUT_LIMIT);
    Object sourceInput = canonicalSourceInput(execution);
    Object transitionInput = new ExecutionInputSnapshot(
        ExecutionInputShape.UNI, continuation.inputFor(sourceInput, region.checkpoint(), limit));
    String transitionKey = pageTransitionKey(region, limit);
    TransitionWorkerCommand command = new TransitionWorkerCommand(
        execution.tenantId(), execution.executionId(), 0, 1, execution.attempt(), ExecutionResultShape.SINGLE,
        execution.version(), transitionKey, transitionInput);
    TransitionCommandEnvelope envelope = TransitionCommandEnvelope.from(
        command, execution.pipelineId(), execution.contractVersion(), execution.releaseVersion(), transitionKey,
        payloadCodec.encode(transitionInput));
    return awaitCoordinator.ensureStreamRegionUnit(
            binding.descriptor(), execution.tenantId(), execution.executionId(), binding.stepIndex())
        .chain(unit -> transitionWorkerExecutor.execute(worker, envelope)
            .onItem().transformToUni(result -> commitWorkerPage(region, binding, unit, result, nowEpochMs)));
  }

  private Uni<Void> commitWorkerPage(
      StreamRegionRecord region,
      StreamRegionAwaitBinding binding,
      AwaitUnitRecord unit,
      TransitionResultEnvelope workerResult,
      long nowEpochMs) {
    StreamRegionContinuationResult page = pageResult(workerResult);
    List<AwaitInteractionRecord> interactions = new ArrayList<>(page.items().size());
    for (int offset = 0; offset < page.items().size(); offset++) {
      int ordinal = Math.toIntExact(region.nextLogicalOrdinal() + offset);
      String interactionId = StreamRegionPageCommit.interactionId(region, ordinal);
      interactions.add(awaitCoordinator.streamRegionInteraction(
          binding.descriptor(), unit, interactionId, page.items().get(offset), ordinal, nowEpochMs)
          .linkedToStreamRegion(region.regionId()));
    }
    StreamRegionPageCommit commit = new StreamRegionPageCommit(
        region, page.nextCheckpoint(), page.endOfSource(), interactions, nowEpochMs);
    return awaitCoordinator.materializeStreamRegionPage(commit)
        .onItem().transformToUni(ignored -> dispatchCommitted(binding, interactions));
  }

  private Uni<Void> dispatchCommitted(StreamRegionAwaitBinding binding, List<AwaitInteractionRecord> interactions) {
    return io.smallrye.mutiny.Multi.createFrom().iterable(interactions)
        .onItem().transformToUniAndConcatenate(interaction -> awaitCoordinator.dispatch(binding.descriptor(), interaction))
        .collect().asList()
        .replaceWithVoid();
  }

  private StreamRegionContinuationResult pageResult(TransitionResultEnvelope workerResult) {
    if (workerResult.outcome() != TransitionWorkerOutcome.COMPLETED) {
      throw new IllegalStateException("Stream region transition did not complete: " + workerResult.outcome());
    }
    List<?> outputs = workerResult.decodeOutputItems(payloadCodec);
    if (outputs.size() != 1 || !(outputs.getFirst() instanceof StreamRegionContinuationResult page)) {
      throw new IllegalStateException("Stream region transition must return exactly one typed page result");
    }
    return page;
  }

  private static Object canonicalSourceInput(ExecutionRecord<Object, Object> execution) {
    Object input = execution.inputPayload();
    if (input instanceof ExecutionInputSnapshot snapshot) {
      if (snapshot.shape() != ExecutionInputShape.UNI) {
        throw new IllegalStateException("A resumable producer requires one canonical source input");
      }
      return snapshot.payload();
    }
    return input;
  }

  private static String pageTransitionKey(StreamRegionRecord region, int limit) {
    String identity = region.tenantId() + "\n" + region.executionId() + "\n" + region.regionId() + "\n"
        + region.source().fingerprint() + "\n" + region.checkpoint().value().orElse("") + "\n"
        + region.nextLogicalOrdinal() + "\n" + limit;
    return "stream-region-page:" + UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8));
  }
}
