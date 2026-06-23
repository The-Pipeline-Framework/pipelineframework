package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.awaitable.AwaitExecutionContext;
import org.pipelineframework.awaitable.AwaitExecutionContextHolder;
import org.pipelineframework.orchestrator.OrchestratorMode;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.step.NonRetryableException;

class CommandStepSupportTest {
  private final InMemoryCommandEffectStore store = new InMemoryCommandEffectStore();
  private final RecordingConnector connector = new RecordingConnector();
  private final CommandStepSupport support = new CommandStepSupport(
      List.of(connector),
      List.of(store),
      config(OrchestratorMode.QUEUE_ASYNC));
  private final CommandDescriptor descriptor = new CommandDescriptor(
      "ProcessWriteSearchIndexDocumentService",
      "opensearch-index-document",
      "Input",
      "Output",
      StaticCommandIdGenerator.class.getName(),
      CommandDuplicatePolicy.RETURN_RECORDED,
      Map.of());

  @AfterEach
  void clearContext() {
    AwaitExecutionContextHolder.clear();
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
        config(OrchestratorMode.SYNC));

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
        config(OrchestratorMode.QUEUE_ASYNC));

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
  }

  private PipelineOrchestratorConfig config(OrchestratorMode mode) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    when(config.mode()).thenReturn(mode);
    return config;
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
