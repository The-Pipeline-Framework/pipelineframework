package org.pipelineframework.orchestrator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Test-only factory that exposes the package-scoped Dynamo store constructor to lifecycle tests. */
public final class DynamoAwaitLifecycleTestStores {

  private DynamoAwaitLifecycleTestStores() {
  }

  public static ExecutionStateStore executionStore(DynamoDbClient client, String prefix) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(config.executionTtlDays()).thenReturn(1);
    when(dynamo.executionTable()).thenReturn(prefix + "_execution");
    when(dynamo.executionKeyTable()).thenReturn(prefix + "_execution_key");
    when(dynamo.executionPayloadTable()).thenReturn(prefix + "_execution_payload");
    return new DynamoExecutionStateStore(client, config);
  }

  /** Read/release recovery does not need creation TTL, idempotency-key, or external-payload tables. */
  public static ExecutionStateStore executionStoreForExistingState(DynamoDbClient client, String prefix) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(dynamo.executionTable()).thenReturn(prefix + "_execution");
    return new DynamoExecutionStateStore(client, config);
  }

  /** Initial creation only requires the execution and idempotency-key tables. */
  public static ExecutionStateStore executionStoreForCreate(DynamoDbClient client, String prefix) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(dynamo.executionTable()).thenReturn(prefix + "_execution");
    when(dynamo.executionKeyTable()).thenReturn(prefix + "_execution_key");
    return new DynamoExecutionStateStore(client, config);
  }

  /** Release mutations may externalize a new canonical input payload. */
  public static ExecutionStateStore executionStoreForPayloadMutation(DynamoDbClient client, String prefix) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(dynamo.executionTable()).thenReturn(prefix + "_execution");
    when(dynamo.executionPayloadTable()).thenReturn(prefix + "_execution_payload");
    return new DynamoExecutionStateStore(client, config);
  }
}
