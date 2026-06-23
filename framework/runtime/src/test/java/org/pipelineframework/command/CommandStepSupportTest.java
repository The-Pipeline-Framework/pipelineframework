package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.config.PipelineStepConfig;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.NonRetryableException;
import org.pipelineframework.telemetry.PipelineTelemetry;

class CommandStepSupportTest {
  private static final String TRANSITION_TOTAL = "tpf.command.effect.transition.total";
  private static final String DUPLICATE_TOTAL = "tpf.command.effect.duplicate.total";
  private static final String DURATION = "tpf.command.effect.duration";

  private final InMemoryCommandEffectStore store = new InMemoryCommandEffectStore();
  private final RecordingConnector connector = new RecordingConnector();
  private InMemoryMetricReader metricReader;
  private SdkMeterProvider meterProvider;
  private CommandStepSupport support;
  private final CommandDescriptor descriptor = new CommandDescriptor(
      "ProcessWriteSearchIndexDocumentService",
      "opensearch-index-document",
      "Input",
      "Output",
      StaticCommandIdGenerator.class.getName(),
      CommandDuplicatePolicy.RETURN_RECORDED,
      Map.of());

  @BeforeEach
  void setUpMetrics() {
    metricReader = InMemoryMetricReader.create();
    meterProvider = SdkMeterProvider.builder()
        .registerMetricReader(metricReader)
        .build();
    OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
        .setMeterProvider(meterProvider)
        .build();
    GlobalOpenTelemetry.resetForTest();
    GlobalOpenTelemetry.set(sdk);
    support = new CommandStepSupport(
        List.of(connector),
        List.of(store),
        config(OrchestratorMode.QUEUE_ASYNC),
        telemetry(true));
  }

  @AfterEach
  void clearContext() {
    AwaitExecutionContextHolder.clear();
    if (meterProvider != null) {
      meterProvider.shutdown();
    }
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  void recordsEffectAndReturnsConnectorOutput() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));

    CommandOutput output = support
        .<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), new CommandInput("doc-1"))
        .await().atMost(Duration.ofSeconds(5));

    assertEquals("cmd-doc-1", output.commandId);
    assertEquals(1, connector.calls.get());
    CommandEffectRecord record = store.find("tenant", "cmd-doc-1").await().atMost(Duration.ofSeconds(5)).orElseThrow();
    assertEquals(CommandEffectStatus.SUCCEEDED, record.status());
    assertSame(output, record.output());
    assertTransitionCount("pending", 1);
    assertTransitionCount("dispatching", 1);
    assertTransitionCount("succeeded", 1);
    assertDurationCount("succeeded", 1);
  }

  @Test
  void returnRecordedDuplicateDoesNotReissueConnector() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    CommandInput input = new CommandInput("doc-1");

    support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), input)
        .await().atMost(Duration.ofSeconds(5));
    CommandOutput duplicate = support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), input)
        .await().atMost(Duration.ofSeconds(5));

    assertEquals(1, connector.calls.get());
    assertEquals(Boolean.TRUE, duplicate.recordedDuplicate);
    assertDuplicateCount("RETURN_RECORDED", "returned_recorded", 1);
  }

  @Test
  void failDuplicatePolicyDoesNotReturnRecordedOutput() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    CommandInput input = new CommandInput("doc-1");
    CommandDescriptor failDuplicate = new CommandDescriptor(
        descriptor.stepId(),
        descriptor.command(),
        descriptor.inputType(),
        descriptor.outputType(),
        descriptor.commandIdGenerator(),
        CommandDuplicatePolicy.FAIL,
        Map.of());

    support.<CommandInput, CommandOutput>execute(failDuplicate, new StaticCommandIdGenerator(), input)
        .await().atMost(Duration.ofSeconds(5));
    NonRetryableException error = assertThrows(NonRetryableException.class,
        () -> support.<CommandInput, CommandOutput>execute(failDuplicate, new StaticCommandIdGenerator(), input)
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Duplicate command completion for commandId cmd-doc-1", error.getMessage());
    assertEquals(1, connector.calls.get());
    assertDuplicateCount("FAIL", "rejected", 1);
  }

  @Test
  void inProgressDuplicateEmitsDuplicateMetric() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    CommandRequest<CommandInput> request = new CommandRequest<>(
        descriptor,
        "cmd-doc-1",
        new CommandInput("doc-1"),
        new AwaitExecutionContext("tenant", "exec-1", 4),
        Map.of());
    store.createPending(request, System.currentTimeMillis()).await().atMost(Duration.ofSeconds(5));

    CommandInProgressException error = assertThrows(CommandInProgressException.class,
        () -> support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Command already in progress for commandId cmd-doc-1", error.getMessage());
    assertEquals(0, connector.calls.get());
    assertDuplicateCount("RETURN_RECORDED", "in_progress", 1);
  }

  @Test
  void failsWithoutQueueAsyncContext() {
    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Command step executed without queue-async execution context.", error.getMessage());
  }

  @Test
  void failsWhenOrchestratorModeIsNotQueueAsync() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    CommandStepSupport syncSupport = new CommandStepSupport(
        List.of(connector),
        List.of(store),
        config(OrchestratorMode.SYNC),
        telemetry(true));

    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> syncSupport.<CommandInput, CommandOutput>execute(
                descriptor,
                new StaticCommandIdGenerator(),
                new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Command steps require pipeline.orchestrator.mode=QUEUE_ASYNC.", error.getMessage());
  }

  @Test
  void rejectsMultipleEffectStores() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    CommandStepSupport duplicateStoreSupport = new CommandStepSupport(
        List.of(connector),
        List.of(store, new InMemoryCommandEffectStore()),
        config(OrchestratorMode.QUEUE_ASYNC),
        telemetry(true));

    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> duplicateStoreSupport.<CommandInput, CommandOutput>execute(
                descriptor,
                new StaticCommandIdGenerator(),
                new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Multiple CommandEffectStore instances configured; command steps support a single effect store",
        error.getMessage());
  }

  @Test
  void rejectsCommandIdWithLeadingOrTrailingWhitespace() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));

    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> support.<CommandInput, CommandOutput>execute(
                descriptor,
                (ignored, input) -> " cmd-" + input.id() + " ",
                new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("Command id generator " + StaticCommandIdGenerator.class.getName()
        + " returned a command id with leading or trailing whitespace", error.getMessage());
  }

  @Test
  void retryableConnectorFailureRecordsRetryableFailure() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    connector.failure = new IllegalStateException("opensearch unavailable");

    IllegalStateException error = assertThrows(IllegalStateException.class,
        () -> support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("opensearch unavailable", error.getMessage());
    CommandEffectRecord record = store.find("tenant", "cmd-doc-1").await().atMost(Duration.ofSeconds(5)).orElseThrow();
    assertEquals(CommandEffectStatus.FAILED_RETRYABLE, record.status());
    assertEquals(IllegalStateException.class.getName(), record.errorClass());
    assertEquals("opensearch unavailable", record.errorMessage());
    assertTransitionCount("failed_retryable", 1);
    assertDurationCount("failed_retryable", 1);
  }

  @Test
  void nonRetryableConnectorFailureRecordsDlq() {
    AwaitExecutionContextHolder.set(new AwaitExecutionContext("tenant", "exec-1", 4));
    connector.failure = new NonRetryableException("invalid index document");

    NonRetryableException error = assertThrows(NonRetryableException.class,
        () -> support.<CommandInput, CommandOutput>execute(descriptor, new StaticCommandIdGenerator(), new CommandInput("doc-1"))
            .await().atMost(Duration.ofSeconds(5)));

    assertEquals("invalid index document", error.getMessage());
    CommandEffectRecord record = store.find("tenant", "cmd-doc-1").await().atMost(Duration.ofSeconds(5)).orElseThrow();
    assertEquals(CommandEffectStatus.DLQ, record.status());
    assertEquals(NonRetryableException.class.getName(), record.errorClass());
    assertEquals("invalid index document", record.errorMessage());
    assertTransitionCount("dlq", 1);
    assertDurationCount("dlq", 1);
  }

  private void assertTransitionCount(String status, long expected) {
    long actual = longSumPointValue(
        TRANSITION_TOTAL,
        Map.of(
            "tpf.command", "opensearch-index-document",
            "tpf.command.step", "ProcessWriteSearchIndexDocumentService",
            "tpf.command.status", status));
    assertEquals(expected, actual);
  }

  private void assertDuplicateCount(String duplicatePolicy, String duplicateResult, long expected) {
    long actual = longSumPointValue(
        DUPLICATE_TOTAL,
        Map.of(
            "tpf.command", "opensearch-index-document",
            "tpf.command.step", "ProcessWriteSearchIndexDocumentService",
            "tpf.command.duplicate_policy", duplicatePolicy,
            "tpf.command.duplicate_result", duplicateResult));
    assertEquals(expected, actual);
  }

  private void assertDurationCount(String status, long expected) {
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    MetricData metric = metrics.stream()
        .filter(candidate -> DURATION.equals(candidate.getName()))
        .findFirst()
        .orElseThrow();
    long actual = metric.getHistogramData().getPoints().stream()
        .filter(point -> attributesMatch(point.getAttributes().asMap(), Map.of(
            "tpf.command", "opensearch-index-document",
            "tpf.command.step", "ProcessWriteSearchIndexDocumentService",
            "tpf.command.status", status)))
        .findFirst()
        .orElseThrow()
        .getCount();
    assertEquals(expected, actual);
  }

  private long longSumPointValue(String metricName, Map<String, String> expectedAttributes) {
    Collection<MetricData> metrics = metricReader.collectAllMetrics();
    MetricData metric = metrics.stream()
        .filter(candidate -> metricName.equals(candidate.getName()))
        .findFirst()
        .orElseThrow();
    return metric.getLongSumData().getPoints().stream()
        .filter(point -> attributesMatch(point.getAttributes().asMap(), expectedAttributes))
        .findFirst()
        .orElseThrow()
        .getValue();
  }

  private boolean attributesMatch(
      Map<AttributeKey<?>, Object> actualAttributes,
      Map<String, String> expectedAttributes
  ) {
    return expectedAttributes.entrySet().stream()
        .allMatch(entry -> entry.getValue().equals(actualAttributes.get(AttributeKey.stringKey(entry.getKey()))));
  }

  private PipelineOrchestratorConfig config(OrchestratorMode mode) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(mode);
    return config;
  }

  private PipelineTelemetry telemetry(boolean metricsEnabled) {
    PipelineStepConfig stepConfig = mock(PipelineStepConfig.class);
    PipelineStepConfig.TelemetryConfig telemetryConfig = mock(PipelineStepConfig.TelemetryConfig.class);
    PipelineStepConfig.TracingConfig tracingConfig = mock(PipelineStepConfig.TracingConfig.class);
    PipelineStepConfig.MetricsConfig metricsConfig = mock(PipelineStepConfig.MetricsConfig.class);
    PipelineStepConfig.ReplayConfig replayConfig = mock(PipelineStepConfig.ReplayConfig.class);
    when(stepConfig.telemetry()).thenReturn(telemetryConfig);
    when(stepConfig.killSwitch()).thenReturn(null);
    when(telemetryConfig.enabled()).thenReturn(true);
    when(telemetryConfig.tracing()).thenReturn(tracingConfig);
    when(telemetryConfig.metrics()).thenReturn(metricsConfig);
    when(telemetryConfig.replay()).thenReturn(replayConfig);
    when(tracingConfig.enabled()).thenReturn(false);
    when(tracingConfig.perItem()).thenReturn(false);
    when(metricsConfig.enabled()).thenReturn(metricsEnabled);
    when(replayConfig.enabled()).thenReturn(false);
    when(replayConfig.exporter()).thenReturn("none");
    when(replayConfig.filePath()).thenReturn(Optional.empty());
    return new PipelineTelemetry(stepConfig);
  }

  record CommandInput(String id) {
  }

  static class CommandOutput {
    String commandId;
    Boolean recordedDuplicate;

    public void setRecordedDuplicate(Boolean recordedDuplicate) {
      this.recordedDuplicate = recordedDuplicate;
    }
  }

  static class StaticCommandIdGenerator implements CommandIdGenerator<CommandInput> {
    @Override
    public String commandId(CommandDescriptor descriptor, CommandInput input) {
      return "cmd-" + input.id();
    }
  }

  static class RecordingConnector implements CommandConnector<CommandInput, CommandOutput> {
    final AtomicInteger calls = new AtomicInteger();
    RuntimeException failure;

    @Override
    public String command() {
      return "opensearch-index-document";
    }

    @Override
    public Uni<CommandOutput> execute(CommandRequest<CommandInput> request) {
      calls.incrementAndGet();
      if (failure != null) {
        return Uni.createFrom().failure(failure);
      }
      CommandOutput output = new CommandOutput();
      output.commandId = request.commandId();
      output.recordedDuplicate = false;
      return Uni.createFrom().item(output);
    }
  }
}
