package org.pipelineframework.awaitable.store;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.spi.AwaitUnitStore;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/** Test-only factory which keeps the package-private Dynamo store constructor out of lifecycle tests. */
public final class DynamoAwaitLifecycleTestStores {

  private DynamoAwaitLifecycleTestStores() {
  }

  public static AwaitInteractionStore interactionStore(DynamoDbClient client, String tablePrefix) {
    return interactionStore(client, tablePrefix, true);
  }

  /** A completion/read-only store does not need to resolve idempotency or correlation lookup keys. */
  public static AwaitInteractionStore interactionStoreForCompletion(DynamoDbClient client, String tablePrefix) {
    return interactionStore(client, tablePrefix, false);
  }

  /** A separate store instance models a fresh runtime recovering await-unit state from Dynamo. */
  public static AwaitUnitStore unitStore(DynamoDbClient client, String tablePrefix) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(dynamo.awaitUnitTable()).thenReturn(tablePrefix + "_unit");
    return new DynamoAwaitUnitStore(client, config);
  }

  private static AwaitInteractionStore interactionStore(DynamoDbClient client, String tablePrefix, boolean lookupRequired) {
    PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
    when(config.dynamo()).thenReturn(dynamo);
    when(dynamo.awaitInteractionTable()).thenReturn(tablePrefix + "_interaction");
    if (lookupRequired) {
      when(dynamo.awaitInteractionKeyTable()).thenReturn(tablePrefix + "_interaction_key");
    }
    return new DynamoAwaitInteractionStore(client, config);
  }
}
