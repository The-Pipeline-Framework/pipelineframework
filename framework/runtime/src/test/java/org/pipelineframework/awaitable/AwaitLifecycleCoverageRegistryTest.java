package org.pipelineframework.awaitable;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class AwaitLifecycleCoverageRegistryTest {

  @Test
  void everyDeclaredObligationIsCoveredByAnExplicitJourney() {
    List<AwaitLifecycleCoverageRegistry.Journey> journeys = AwaitLifecycleCoverageRegistry.journeys();
    assertCoverage("transition", ids(AwaitLifecycleCoverageRegistry.transitions(), AwaitLifecycleCoverageRegistry.TransitionObligation::id), journeys.stream()
        .flatMap(journey -> journey.transitionObligationIds().stream()).collect(Collectors.toSet()));
    assertCoverage("crash", ids(AwaitLifecycleCoverageRegistry.crashes(), AwaitLifecycleCoverageRegistry.CrashObligation::id), journeys.stream()
        .flatMap(journey -> journey.crashObligationIds().stream()).collect(Collectors.toSet()));
    assertCoverage("race", ids(AwaitLifecycleCoverageRegistry.races(), AwaitLifecycleCoverageRegistry.RaceObligation::id), journeys.stream()
        .flatMap(journey -> journey.raceObligationIds().stream()).collect(Collectors.toSet()));
  }

  @Test
  void coverageReportSeparatesImplementedJourneysFromPlannedJourneys() {
    AwaitLifecycleCoverageRegistry.CoverageReport report = AwaitLifecycleCoverageRegistry.coverageReport();

    assertEquals(Set.of(
        "scalar_completion_after_restart_resumes_once",
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
        "many_to_many_partial_replay_holds_parent",
        "many_to_many_parent_release_after_restart",
        "sequential_durable_shape_uninterrupted",
        "sequential_durable_shape_after_restart",
        "terminal_cleanup_durable_state"), report.implementedJourneys());
    assertFalse(report.uncoveredTransitions().contains("completion-persisted"));
    assertFalse(report.uncoveredTransitions().contains("dispatch-admitted"));
    assertFalse(report.uncoveredTransitions().contains("item-parent-release"));
    assertFalse(report.uncoveredCrashes().contains("after-transition-admission"));
    assertFalse(report.uncoveredCrashes().contains("after-dispatch"));
    assertFalse(report.uncoveredTransitions().contains("request-persisted"));
    assertFalse(report.uncoveredTransitions().contains("parent-release-guard"));
    assertFalse(report.uncoveredTransitions().contains("terminal-cleanup"));
    assertFalse(report.uncoveredCrashes().contains("after-parent-release"));
    assertFalse(report.uncoveredCrashes().contains("after-next-state-persisted"));
    assertFalse(report.uncoveredRaces().contains("duplicate-completion"));
    assertFalse(report.uncoveredRaces().contains("conflicting-completion"));
    assertFalse(report.uncoveredRaces().contains("timeout-completion"));
    assertFalse(report.uncoveredRaces().contains("cancellation-completion"));
    assertFalse(report.uncoveredShapes().contains(AwaitLifecycleCoverageRegistry.AwaitShape.EMPTY_ITEMIZED_ONE_TO_ONE));
    assertFalse(report.uncoveredShapes().contains(AwaitLifecycleCoverageRegistry.AwaitShape.ONE_TO_MANY));
    assertFalse(report.uncoveredShapes().contains(AwaitLifecycleCoverageRegistry.AwaitShape.MANY_TO_ONE));
    assertFalse(report.uncoveredShapes().contains(AwaitLifecycleCoverageRegistry.AwaitShape.SEQUENTIAL_AWAITS));
    assertEquals(EnumSet.noneOf(AwaitLifecycleCoverageRegistry.CompletionTransport.class),
        report.uncoveredTransportAnchors());
    assertEquals(Set.of(), report.uncoveredTransitions());
    assertEquals(Set.of(), report.uncoveredCrashes());
    assertEquals(Set.of(), report.uncoveredRaces());
    assertEquals(Set.of(), report.uncoveredShapes());
  }

  @Test
  void everyShapeHasUninterruptedAndRestartedCoverage() {
    Map<AwaitLifecycleCoverageRegistry.AwaitShape, Set<AwaitLifecycleCoverageRegistry.JourneyMode>> modes = AwaitLifecycleCoverageRegistry.journeys().stream()
        .collect(Collectors.groupingBy(AwaitLifecycleCoverageRegistry.Journey::shape,
            Collectors.mapping(AwaitLifecycleCoverageRegistry.Journey::mode, Collectors.toSet())));
    for (AwaitLifecycleCoverageRegistry.AwaitShape shape : AwaitLifecycleCoverageRegistry.AwaitShape.values()) {
      assertEquals(EnumSet.allOf(AwaitLifecycleCoverageRegistry.JourneyMode.class), modes.get(shape),
          () -> "Missing uninterrupted or restarted journey for " + shape);
    }
  }

  @Test
  void eachCompletionTransportHasExactlyOneAdmissionAnchor() {
    Map<AwaitLifecycleCoverageRegistry.CompletionTransport, Long> anchors = AwaitLifecycleCoverageRegistry.journeys().stream()
        .map(AwaitLifecycleCoverageRegistry.Journey::transportAnchor)
        .filter(java.util.Objects::nonNull)
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    for (AwaitLifecycleCoverageRegistry.CompletionTransport transport : AwaitLifecycleCoverageRegistry.CompletionTransport.values()) {
      assertEquals(1L, anchors.getOrDefault(transport, 0L), () -> "Missing or duplicate anchor for " + transport);
    }
  }

  @Test
  void journeyDefinitionsAreStableAndReferenceKnownObligations() {
    Set<String> transitionIds = ids(AwaitLifecycleCoverageRegistry.transitions(), AwaitLifecycleCoverageRegistry.TransitionObligation::id);
    Set<String> crashIds = ids(AwaitLifecycleCoverageRegistry.crashes(), AwaitLifecycleCoverageRegistry.CrashObligation::id);
    Set<String> raceIds = ids(AwaitLifecycleCoverageRegistry.races(), AwaitLifecycleCoverageRegistry.RaceObligation::id);
    Set<String> journeyNames = AwaitLifecycleCoverageRegistry.journeys().stream().map(AwaitLifecycleCoverageRegistry.Journey::name).collect(Collectors.toSet());
    assertEquals(AwaitLifecycleCoverageRegistry.journeys().size(), journeyNames.size(), "Journey names must be unique");
    for (AwaitLifecycleCoverageRegistry.Journey journey : AwaitLifecycleCoverageRegistry.journeys()) {
      assertFalse(journey.name().isBlank());
      assertFalse(journey.fixtureScenario().isBlank());
      assertTrue(transitionIds.containsAll(journey.transitionObligationIds()), () -> "Unknown transition in " + journey.name());
      assertTrue(crashIds.containsAll(journey.crashObligationIds()), () -> "Unknown crash in " + journey.name());
      assertTrue(raceIds.containsAll(journey.raceObligationIds()), () -> "Unknown race in " + journey.name());
    }
  }

  @Test
  void crashAndRaceObligationsDescribeDurableReconstruction() {
    for (AwaitLifecycleCoverageRegistry.CrashObligation crash : AwaitLifecycleCoverageRegistry.crashes()) {
      assertFalse(crash.discardedProcessState().isEmpty(), () -> "Crash must discard process state: " + crash.id());
      assertFalse(crash.recoveryTrigger().isBlank(), () -> "Crash must declare recovery trigger: " + crash.id());
      assertFalse(crash.reconstructionAction().isBlank(), () -> "Crash must declare reconstruction: " + crash.id());
    }
    for (AwaitLifecycleCoverageRegistry.RaceObligation race : AwaitLifecycleCoverageRegistry.races()) {
      assertTrue(race.contenders().size() >= 2, () -> "Race must name its contenders: " + race.id());
      assertFalse(race.validDurableOutcomes().isEmpty(), () -> "Race must bound durable outcomes: " + race.id());
      assertFalse(race.convergedSemanticResult().isBlank(), () -> "Race must define convergence: " + race.id());
    }
  }

  private static <T> Set<String> ids(List<T> obligations, Function<T, String> id) {
    Set<String> ids = obligations.stream().map(id).collect(Collectors.toSet());
    assertEquals(obligations.size(), ids.size(), "Obligation IDs must be unique");
    return ids;
  }

  private static void assertCoverage(String kind, Set<String> declared, Set<String> covered) {
    Set<String> missing = declared.stream().filter(id -> !covered.contains(id)).collect(Collectors.toSet());
    assertTrue(missing.isEmpty(), () -> "Uncovered " + kind + " obligations: " + missing);
  }
}
