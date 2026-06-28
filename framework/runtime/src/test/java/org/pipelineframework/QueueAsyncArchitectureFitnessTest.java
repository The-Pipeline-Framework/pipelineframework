package org.pipelineframework;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueAsyncArchitectureFitnessTest {

  private static final List<String> PURE_MODEL_FILES = List.of(
      "ClaimedSegment",
      "SegmentCommitPlan",
      "CompletedSegment",
      "SuspendedSegment",
      "FailedSegment",
      "TerminalPublicationPlan",
      "AwaitContinuationPlan",
      "AwaitContinuationPlanner",
      "ItemContinuationKey",
      "ItemizedParentRelease");

  private static final Pattern FORBIDDEN_PURE_IMPORT = Pattern.compile(
      "^import\\s+.*("
          + "Store|Dispatcher|Publisher|CheckpointPublicationService|ObjectPublishCompletionService|"
          + "AwaitCoordinator|SegmentBoundaryLedger|jakarta\\.|javax\\.|Logger|"
          + "io\\.smallrye\\.mutiny\\.|\\.Uni|\\.Multi"
          + ").*;",
      Pattern.MULTILINE);

  private static final Pattern FORBIDDEN_PURE_INVOCATION = Pattern.compile(
      "\\.\\s*(mark\\w*|enqueue\\w*|publish\\w*|dispatch(?!Complete\\(|ClaimKey\\()\\w*|subscribe)\\s*\\(");

  @Test
  void pureQueueAsyncModelsDoNotImportOrInvokeImperativeShells() throws IOException {
    for (String type : PURE_MODEL_FILES) {
      Path source = source(type);
      if (!Files.exists(source)) {
        continue;
      }
      String code = stripComments(Files.readString(source));

      Matcher importMatcher = FORBIDDEN_PURE_IMPORT.matcher(code);
      assertFalse(
          importMatcher.find(),
          () -> type + " must stay pure; forbidden import: " + importMatcher.group());

      Matcher invocationMatcher = FORBIDDEN_PURE_INVOCATION.matcher(code);
      assertFalse(
          invocationMatcher.find(),
          () -> type + " must not perform shell effects; forbidden invocation: " + invocationMatcher.group());
    }
  }

  @Test
  void coordinatorStaysFacadeForSegmentAndAwaitCompletionPaths() throws IOException {
    String coordinator = Files.readString(source("QueueAsyncCoordinator"));

    assertFalse(coordinator.contains("TransitionWorkerOutcome"),
        "QueueAsyncCoordinator must not branch on worker outcomes");
    assertFalse(coordinator.contains("TransitionResultEnvelope"),
        "QueueAsyncCoordinator must not branch on transition result envelopes");
    assertAbsent(coordinator, "handleCompletedTransition");
    assertAbsent(coordinator, "prepareTransitionCommand");
    assertAbsent(coordinator, "runAsyncExecution");
    assertAbsent(coordinator, "markWaitingExternal");
    assertAbsent(coordinator, "publishTerminalOutputsIfConfigured");

    assertTrue(
        coordinator.contains("return segmentPipeline().process(workItem, worker, itemContinuationHandler);"),
        "processExecutionWorkItem must delegate to QueueAsyncSegmentPipeline");
    assertTrue(
        coordinator.contains("return awaitBoundaryAdmission().complete(command, itemContinuationHandler);"),
        "completeAwait must delegate to AwaitBoundaryAdmission");
  }

  @Test
  void effectCallsStayBehindExplicitBoundaries() throws IOException {
    assertOnlyFileCalls("publishIfConfigured", "TerminalPublicationBoundary");

    String segmentEffects = Files.readString(source("SegmentCommitEffects"));
    assertTrue(segmentEffects.contains(".markSucceeded("),
        "SegmentCommitEffects owns transition success projection writes");
    assertTrue(segmentEffects.contains(".markWaitingExternal("),
        "SegmentCommitEffects owns transition suspend projection writes");

    for (String type : PURE_MODEL_FILES) {
      Path source = source(type);
      if (!Files.exists(source)) {
        continue;
      }
      String code = stripComments(Files.readString(source));
      assertFalse(code.contains(".markSucceeded("), type + " must not mark transition success");
      assertFalse(code.contains(".markWaitingExternal("), type + " must not mark transition suspension");
      assertFalse(code.contains(".markTerminalFailure("), type + " must not mark transition failure");
    }
  }

  @Test
  void segmentPipelineHasSingleSubscriptionBoundaryAndNoManualSubscribe() throws IOException {
    String pipeline = Files.readString(source("QueueAsyncSegmentPipeline"));

    assertEquals(
        1,
        occurrences(pipeline, "Uni.createFrom().deferred("),
        "QueueAsyncSegmentPipeline should have exactly one per-subscription deferred boundary");
    assertFalse(pipeline.contains(".subscribe("), "QueueAsyncSegmentPipeline must not subscribe manually");
  }

  @Test
  void queueAsyncClassesStayBelowGodClassGuardrails() throws IOException {
    assertLineCountAtMost("QueueAsyncCoordinator", 1050);
    assertLineCountAtMost("AwaitContinuations", 900);
    assertLineCountAtMost("QueueAsyncSegmentPipeline", 275);
    assertLineCountAtMost("SegmentCommitEffects", 250);
    assertLineCountAtMost("TerminalPublicationBoundary", 175);
  }

  private static void assertAbsent(String code, String token) {
    assertFalse(code.contains(token), () -> token + " should not be reintroduced in QueueAsyncCoordinator");
  }

  private static void assertOnlyFileCalls(String invocation, String allowedType) throws IOException {
    Path packageRoot = sourceRoot().resolve("org/pipelineframework");
    try (var files = Files.walk(packageRoot)) {
      List<Path> offenders = files
          .filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().endsWith(".java"))
          .filter(path -> !path.getFileName().toString().equals(allowedType + ".java"))
          .filter(path -> contains(path, "." + invocation + "("))
          .toList();
      assertTrue(offenders.isEmpty(), () -> invocation + " may only be called by " + allowedType + ": " + offenders);
    }
  }

  private static boolean contains(Path path, String token) {
    try {
      return stripComments(Files.readString(path)).contains(token);
    } catch (IOException e) {
      throw new IllegalStateException("Failed reading " + path, e);
    }
  }

  private static int occurrences(String code, String token) {
    int count = 0;
    int index = 0;
    while ((index = code.indexOf(token, index)) >= 0) {
      count++;
      index += token.length();
    }
    return count;
  }

  private static void assertLineCountAtMost(String type, int maxLines) throws IOException {
    long lines;
    try (var sourceLines = Files.lines(source(type))) {
      lines = sourceLines.count();
    }
    assertTrue(lines <= maxLines, () -> type + ".java has " + lines + " lines; limit is " + maxLines);
  }

  private static Path source(String type) {
    return sourceRoot().resolve("org/pipelineframework/" + type + ".java");
  }

  private static Path sourceRoot() {
    Path fromRepoRoot = Path.of("framework/runtime/src/main/java");
    if (Files.exists(fromRepoRoot)) {
      return fromRepoRoot;
    }
    Path fromModuleRoot = Path.of("src/main/java");
    if (Files.exists(fromModuleRoot)) {
      return fromModuleRoot;
    }
    throw new IllegalStateException("Cannot locate runtime source root from " + Path.of("").toAbsolutePath());
  }

  private static String stripComments(String code) {
    return code
        .replaceAll("(?s)/\\*.*?\\*/", "")
        .replaceAll("(?m)//.*$", "");
  }
}
