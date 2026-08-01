package org.pipelineframework.awaitable;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Machine-readable await lifecycle coverage obligations.  Keep this registry explicit: it
 * selects representative journeys instead of creating a Cartesian product of every dimension.
 */
public final class AwaitLifecycleCoverageRegistry {

  private AwaitLifecycleCoverageRegistry() {
  }

  public enum AwaitShape {
    SCALAR_ONE_TO_ONE,
    STREAM_ITEMIZED_ONE_TO_ONE,
    EMPTY_ITEMIZED_ONE_TO_ONE,
    ONE_TO_MANY,
    MANY_TO_ONE,
    MANY_TO_MANY,
    SEQUENTIAL_AWAITS
  }

  public enum JourneyMode {
    UNINTERRUPTED,
    RESTARTED
  }

  public enum CompletionTransport {
    KAFKA,
    SQS,
    WEBHOOK
  }

  public record TransitionObligation(
      String id,
      String durablePrecondition,
      String event,
      String durableMutation,
      String emittedAction,
      String idempotencyIdentity,
      String semanticOutcome,
      String retryBehavior,
      String restartReconstructionRule) {
  }

  public record CrashObligation(
      String id,
      String stateBeforeCrash,
      Set<String> discardedProcessState,
      String recoveryTrigger,
      String reconstructionAction,
      String expectedDurableOutcome) {
  }

  public record RaceObligation(
      String id,
      String deterministicInterleaving,
      Set<String> contenders,
      Set<String> validDurableOutcomes,
      String convergedSemanticResult) {
  }

  public record Journey(
      String name,
      AwaitShape shape,
      JourneyMode mode,
      Set<String> transitionObligationIds,
      Set<String> crashObligationIds,
      Set<String> raceObligationIds,
      Optional<CompletionTransport> transportAnchor,
      String fixtureScenario) {
  }

  /**
   * Explicit implementation state for the suite.  Planned journeys stay in {@link #journeys()} so
   * the registry can identify the next missing proof without pretending that a declaration ran.
   */
  public record CoverageReport(
      Set<String> implementedJourneys,
      Set<String> uncoveredTransitions,
      Set<String> uncoveredCrashes,
      Set<String> uncoveredRaces,
      Set<AwaitShape> uncoveredShapes,
      Set<CompletionTransport> uncoveredTransportAnchors) {
  }

  public static List<TransitionObligation> transitions() {
    return List.of(
        transition("request-persisted", "no interaction", "create request", "pending interaction", "dispatch request", "idempotency key", "one durable request", "create-or-get", "redispatch from interaction"),
        transition("dispatch-admitted", "pending interaction", "dispatch accepted", "interaction remains pending", "provider request", "interaction id", "one provider contract", "retry dispatch", "redispatch from interaction"),
        transition("completion-persisted", "pending interaction", "completion admitted", "completed response", "release evaluation", "interaction id", "one semantic completion", "idempotent completion", "reload interaction and unit"),
        transition("scalar-release", "completed scalar interaction", "release evaluation", "queued parent", "continuation work", "await unit id", "scalar resume", "idempotent release", "reload parent state"),
        transition("item-output-persisted", "completed item interaction", "child continuation", "succeeded child", "parent release evaluation", "child execution key", "one child output", "idempotent child success", "query durable child"),
        transition("parent-release-guard", "one or more required children are non-successful", "release evaluation", "no parent mutation", "none", "await unit id", "parent remains held", "retry after durable child success", "query every required child"),
        transition("item-parent-release", "all children succeeded", "release evaluation", "queued parent", "continuation work", "await unit id", "ordered aggregate resume", "idempotent release", "query every child"),
        transition("continuation-admitted", "queued parent", "worker admission", "next execution state", "business segment", "transition key", "one semantic progress", "lease retry", "reload execution"),
        transition("terminal-cleanup", "terminal execution", "terminalization", "no active await state", "terminal publication", "execution id", "one terminal outcome", "idempotent terminalization", "query no pending state"));
  }

  public static List<CrashObligation> crashes() {
    Set<String> processState = Set.of("coordinator", "live registry", "item claims", "descriptor cache", "scheduler");
    return List.of(
        crash("after-request-persisted", "pending interaction", processState, "recovery sweep", "reload interaction", "request remains dispatchable"),
        crash("after-dispatch", "pending dispatched interaction", processState, "completion admission", "reload interaction", "one completion contract"),
        crash("after-completion-persisted", "completed interaction", processState, "recovery sweep", "reload unit", "parent release evaluated"),
        crash("after-item-output-persisted", "some succeeded children", processState, "final child completion", "query children", "parent remains held or releases"),
        crash("after-parent-release", "queued parent", processState, "work delivery", "reload parent", "one continuation admitted"),
        crash("after-transition-admission", "claimed continuation", processState, "lease recovery", "reload execution", "one semantic progress"),
        crash("after-next-state-persisted", "advanced execution", processState, "replay", "reload execution", "exact next state"));
  }

  public static List<RaceObligation> races() {
    return List.of(
        race("duplicate-completion", "original then duplicate", Set.of("completion-a", "completion-b"), Set.of("completed-once"), "one semantic completion"),
        race("conflicting-completion", "accepted response then conflicting response", Set.of("completion-a", "completion-b"), Set.of("first-completion-retained", "conflict-rejected"), "one semantic completion"),
        race("timeout-completion", "timeout and completion contend", Set.of("timeout", "completion"), Set.of("completed", "timed-out"), "one terminal await outcome"),
        race("cancellation-completion", "cancellation and completion contend", Set.of("cancel", "completion"), Set.of("completed", "cancelled"), "one terminal await outcome"),
        race("final-child-parent-release", "final child and release evaluation contend", Set.of("child-success", "parent-release"), Set.of("parent-held", "parent-queued"), "queued only after every child succeeds"),
        race("concurrent-parent-release", "two workers evaluate all children", Set.of("worker-a", "worker-b"), Set.of("parent-queued"), "one accepted parent release"),
        race("reload-completion", "runtime reload and completion contend", Set.of("reload", "completion"), Set.of("parent-held", "parent-queued"), "durable state decides release"),
        race("replay-partial-item-output", "replay observes partial child set", Set.of("replay", "child-success"), Set.of("parent-held", "parent-queued"), "aggregate only after every child succeeds"));
  }

  public static List<Journey> journeys() {
    return List.of(
        journey("scalar_request_persisted_dispatches_once", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("request-persisted", "dispatch-admitted"), Set.of(), Set.of("duplicate-completion"), Optional.empty(), "scalarDispatch"),
        journey("kafka_completion_admission_anchor", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of(), Optional.of(CompletionTransport.KAFKA), "kafkaDynamoAdmission"),
        journey("scalar_dispatch_after_restart_reconstructs_completion", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.RESTARTED, Set.of("dispatch-admitted"), Set.of("after-dispatch"), Set.of(), Optional.empty(), "scalarDispatchRestart"),
        journey("scalar_completion_after_restart_resumes_once", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.RESTARTED, Set.of("request-persisted", "completion-persisted", "scalar-release", "continuation-admitted"), Set.of("after-completion-persisted"), Set.of("reload-completion"), Optional.empty(), "scalarRestart"),
        journey("scalar_duplicate_completion_race", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("duplicate-completion"), Optional.empty(), "duplicateCompletionRace"),
        journey("scalar_conflicting_completion_race", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("conflicting-completion"), Optional.empty(), "conflictingCompletionRace"),
        journey("scalar_timeout_completion_race", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("timeout-completion"), Optional.empty(), "timeoutCompletionRace"),
        journey("scalar_cancellation_completion_race", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("cancellation-completion"), Optional.empty(), "cancellationCompletionRace"),
        journey("itemized_empty_unit_completes_uninterrupted", AwaitShape.EMPTY_ITEMIZED_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("terminal-cleanup"), Set.of(), Set.of(), Optional.empty(), "emptyItemized"),
        journey("terminal_cleanup_durable_state", AwaitShape.EMPTY_ITEMIZED_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("terminal-cleanup"), Set.of(), Set.of(), Optional.empty(), "terminalCleanup"),
        journey("itemized_empty_unit_after_restart_terminalizes", AwaitShape.EMPTY_ITEMIZED_ONE_TO_ONE, JourneyMode.RESTARTED, Set.of(), Set.of("after-request-persisted", "after-next-state-persisted"), Set.of(), Optional.empty(), "emptyItemizedRestart"),
        journey("itemized_reverse_completion_releases_in_order", AwaitShape.STREAM_ITEMIZED_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("item-output-persisted", "parent-release-guard", "item-parent-release"), Set.of(), Set.of("replay-partial-item-output"), Optional.empty(), "itemizedReverseCompletion"),
        journey("itemized_final_child_after_restart_releases_parent", AwaitShape.STREAM_ITEMIZED_ONE_TO_ONE, JourneyMode.RESTARTED, Set.of("item-output-persisted", "item-parent-release"), Set.of("after-item-output-persisted"), Set.of("final-child-parent-release", "concurrent-parent-release"), Optional.empty(), "itemizedRestart"),
        journey("itemized_transition_admission_after_restart", AwaitShape.STREAM_ITEMIZED_ONE_TO_ONE, JourneyMode.RESTARTED, Set.of("continuation-admitted"), Set.of("after-transition-admission"), Set.of(), Optional.empty(), "transitionAdmissionRestart"),
        journey("one_to_many_completion_timeout_race", AwaitShape.ONE_TO_MANY, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("timeout-completion"), Optional.empty(), "completionTimeoutRace"),
        journey("sqs_completion_admission_anchor", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of(), Optional.of(CompletionTransport.SQS), "sqsDynamoAdmission"),
        journey("one_to_many_dispatch_after_restart", AwaitShape.ONE_TO_MANY, JourneyMode.RESTARTED, Set.of("dispatch-admitted"), Set.of("after-dispatch"), Set.of(), Optional.empty(), "dispatchRestart"),
        journey("one_to_many_durable_shape_uninterrupted", AwaitShape.ONE_TO_MANY, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("timeout-completion"), Optional.empty(), "oneToManyUninterrupted"),
        journey("one_to_many_durable_shape_after_restart", AwaitShape.ONE_TO_MANY, JourneyMode.RESTARTED, Set.of(), Set.of(), Set.of(), Optional.empty(), "oneToManyRestart"),
        journey("many_to_one_cancellation_completion_race", AwaitShape.MANY_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("cancellation-completion"), Optional.empty(), "manyToOneCancellationCompletionRace"),
        journey("webhook_completion_admission_anchor", AwaitShape.SCALAR_ONE_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of(), Optional.of(CompletionTransport.WEBHOOK), "webhookDynamoAdmission"),
        journey("many_to_one_replay_after_next_state_persisted", AwaitShape.MANY_TO_ONE, JourneyMode.RESTARTED, Set.of(), Set.of(), Set.of(), Optional.empty(), "replayRestart"),
        journey("many_to_one_durable_shape_uninterrupted", AwaitShape.MANY_TO_ONE, JourneyMode.UNINTERRUPTED, Set.of("completion-persisted"), Set.of(), Set.of("cancellation-completion"), Optional.empty(), "manyToOneUninterrupted"),
        journey("many_to_one_durable_shape_after_restart", AwaitShape.MANY_TO_ONE, JourneyMode.RESTARTED, Set.of(), Set.of(), Set.of(), Optional.empty(), "manyToOneRestart"),
        journey("many_to_many_partial_replay_holds_parent", AwaitShape.MANY_TO_MANY, JourneyMode.UNINTERRUPTED, Set.of("item-output-persisted", "parent-release-guard"), Set.of(), Set.of("replay-partial-item-output"), Optional.empty(), "partialItemReplay"),
        journey("many_to_many_parent_release_after_restart", AwaitShape.MANY_TO_MANY, JourneyMode.RESTARTED, Set.of("item-parent-release"), Set.of("after-parent-release"), Set.of(), Optional.empty(), "aggregateRestart"),
        journey("sequential_durable_shape_uninterrupted", AwaitShape.SEQUENTIAL_AWAITS, JourneyMode.UNINTERRUPTED, Set.of(), Set.of(), Set.of(), Optional.empty(), "sequentialShapeUninterrupted"),
        journey("sequential_durable_shape_after_restart", AwaitShape.SEQUENTIAL_AWAITS, JourneyMode.RESTARTED, Set.of(), Set.of(), Set.of(), Optional.empty(), "sequentialShapeRestart"),
        journey("sequential_awaits_complete_uninterrupted", AwaitShape.SEQUENTIAL_AWAITS, JourneyMode.UNINTERRUPTED, Set.of("scalar-release", "terminal-cleanup"), Set.of(), Set.of("conflicting-completion"), Optional.empty(), "sequentialAwaits"),
        journey("sequential_awaits_recover_transition_admission", AwaitShape.SEQUENTIAL_AWAITS, JourneyMode.RESTARTED, Set.of("continuation-admitted"), Set.of("after-transition-admission", "after-request-persisted"), Set.of(), Optional.empty(), "sequentialRestart"));
  }

  /**
   * Resolves the declared journey that a concrete test is exercising.  Test implementations must
   * not recreate their own coverage labels because the meta-test can then no longer detect drift.
   */
  public static Journey journeyNamed(String name) {
    return journeys().stream()
        .filter(journey -> journey.name().equals(name))
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("No declared await lifecycle journey named " + name));
  }

  public static Set<String> implementedJourneyNames() {
    return Set.of(
        "scalar_completion_after_restart_resumes_once",
        "scalar_duplicate_completion_race",
        "scalar_conflicting_completion_race",
        "scalar_timeout_completion_race",
        "scalar_cancellation_completion_race",
        "kafka_completion_admission_anchor",
        "sqs_completion_admission_anchor",
        "webhook_completion_admission_anchor",
        "scalar_dispatch_after_restart_reconstructs_completion",
        "one_to_many_durable_shape_uninterrupted",
        "one_to_many_durable_shape_after_restart",
        "many_to_one_durable_shape_uninterrupted",
        "many_to_one_durable_shape_after_restart",
        "itemized_empty_unit_after_restart_terminalizes",
        "itemized_final_child_after_restart_releases_parent",
        "itemized_transition_admission_after_restart",
        "many_to_many_partial_replay_holds_parent",
        "many_to_many_parent_release_after_restart",
        "sequential_durable_shape_uninterrupted",
        "sequential_durable_shape_after_restart",
        "terminal_cleanup_durable_state");
  }

  public static CoverageReport coverageReport() {
    List<Journey> implemented = journeys().stream()
        .filter(journey -> implementedJourneyNames().contains(journey.name()))
        .toList();
    Set<String> transitionCoverage = implemented.stream()
        .flatMap(journey -> journey.transitionObligationIds().stream())
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<String> crashCoverage = implemented.stream()
        .flatMap(journey -> journey.crashObligationIds().stream())
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<String> raceCoverage = implemented.stream()
        .flatMap(journey -> journey.raceObligationIds().stream())
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<AwaitShape> shapeCoverage = implemented.stream()
        .map(Journey::shape)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    Set<CompletionTransport> transportCoverage = implemented.stream()
        .map(Journey::transportAnchor)
        .flatMap(Optional::stream)
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
    return new CoverageReport(
        implementedJourneyNames(),
        missing(ids(transitions(), TransitionObligation::id), transitionCoverage),
        missing(ids(crashes(), CrashObligation::id), crashCoverage),
        missing(ids(races(), RaceObligation::id), raceCoverage),
        missing(java.util.EnumSet.allOf(AwaitShape.class), shapeCoverage),
        missing(java.util.EnumSet.allOf(CompletionTransport.class), transportCoverage));
  }

  private static <T> Set<T> missing(Set<T> declared, Set<T> covered) {
    return declared.stream().filter(value -> !covered.contains(value))
        .collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static <T> Set<String> ids(List<T> obligations, java.util.function.Function<T, String> id) {
    return obligations.stream().map(id).collect(java.util.stream.Collectors.toUnmodifiableSet());
  }

  private static TransitionObligation transition(String id, String precondition, String event, String mutation, String action, String identity, String outcome, String retry, String restart) {
    return new TransitionObligation(id, precondition, event, mutation, action, identity, outcome, retry, restart);
  }

  private static CrashObligation crash(String id, String state, Set<String> discarded, String trigger, String reconstruction, String outcome) {
    return new CrashObligation(id, state, discarded, trigger, reconstruction, outcome);
  }

  private static RaceObligation race(String id, String interleaving, Set<String> contenders, Set<String> outcomes, String result) {
    return new RaceObligation(id, interleaving, contenders, outcomes, result);
  }

  private static Journey journey(String name, AwaitShape shape, JourneyMode mode, Set<String> transitions, Set<String> crashes, Set<String> races, Optional<CompletionTransport> transport, String fixture) {
    return new Journey(name, shape, mode, transitions, crashes, races, transport, fixture);
  }
}
