package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.core.Response;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.orchestrator.dto.HostedAwaitCompletionRequest;
import org.pipelineframework.orchestrator.release.InMemoryPipelineReleaseRegistry;
import org.pipelineframework.orchestrator.release.PipelineReleaseRegistrar;
import org.pipelineframework.orchestrator.worker.PipelineWorkerAvailability;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Real hosted-control-plane webhook completion admission into a fresh Dynamo interaction store.
 * The webhook transport sends a callback to this resource, which is its lifecycle admission seam.
 */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WebhookCompletionDynamoIT {
  private static final String PREFIX = "webhook_anchor";
  private static final String TOKEN = "webhook-anchor-token";

  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
      DockerImageName.parse("localstack/localstack:3.8"))
      .withServices(LocalStackContainer.Service.DYNAMODB);

  private DynamoDbClient dynamo;

  @BeforeAll
  void setUp() {
    dynamo = DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
        .region(Region.of(LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
            LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .build();
    table(PREFIX + "_interaction", "tenant_id", "interaction_id");
    table(PREFIX + "_interaction_key", "lookup_key", null);
  }

  @Test
  void webhookCallbackIsAdmittedIntoDynamoAfterFreshStoreReconstruction() {
    long now = System.currentTimeMillis();
    AwaitInteractionStore creator = DynamoAwaitLifecycleTestStores.interactionStore(dynamo, PREFIX);
    AwaitInteractionRecord pending = creator.createOrGet(new AwaitCreateCommand(
            "tenant-webhook", "execution-webhook", "AwaitDecision", 2, String.class.getName(),
            "cause-webhook", "idem-request-webhook", "corr-webhook", Map.of("request", "value"),
            null, null, "webhook", "unit-webhook", null, now, now + 60_000L,
            now / 1_000L + 3_600L))
        .await().indefinitely().record();

    AwaitInteractionStore admissionStore = DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, PREFIX);
    PipelineExecutionService executionService = mock(PipelineExecutionService.class);
    when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
        .thenAnswer(invocation -> admissionStore.complete(invocation.getArgument(0)));

    JsonTransitionPayloadCodec payloadCodec = new JsonTransitionPayloadCodec();
    HostedPipelineControlPlaneResource resource = resource(executionService, payloadCodec);
    SerializedTransitionPayload responsePayload = payloadCodec.encode(Map.of("decision", "approved"));
    HostedAwaitCompletionRequest request = new HostedAwaitCompletionRequest(
        pending.interactionId(), null, null, "idem-completion-webhook", responsePayload, "provider");

    Response response = resource.completeInteraction(
        pending.tenantId(), "Bearer " + TOKEN, request).await().indefinitely();

    assertEquals(200, response.getStatus());
    AwaitInteractionStore freshStore = DynamoAwaitLifecycleTestStores
        .interactionStoreForCompletion(dynamo, PREFIX);
    AwaitInteractionRecord completed = freshStore.get(pending.tenantId(), pending.interactionId())
        .await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.COMPLETED, completed.status());
    assertEquals(Map.of("decision", "approved"), completed.responsePayload());
  }

  private static HostedPipelineControlPlaneResource resource(
      PipelineExecutionService executionService,
      JsonTransitionPayloadCodec payloadCodec) {
    PipelineOrchestratorConfig orchestratorConfig = mock(PipelineOrchestratorConfig.class);
    PipelineOrchestratorConfig.ControlPlaneConfig controlPlaneConfig = mock(
        PipelineOrchestratorConfig.ControlPlaneConfig.class);
    when(orchestratorConfig.controlPlane()).thenReturn(controlPlaneConfig);
    when(controlPlaneConfig.enabled()).thenReturn(true);
    when(controlPlaneConfig.adminToken()).thenReturn(Optional.of(TOKEN));
    when(controlPlaneConfig.adminTokenRef()).thenReturn(Optional.empty());

    HostedPipelineControlPlaneResource resource = new HostedPipelineControlPlaneResource();
    resource.orchestratorConfig = orchestratorConfig;
    resource.controlPlane = mock(PipelineControlPlane.class);
    resource.executionService = executionService;
    resource.releaseRegistry = new InMemoryPipelineReleaseRegistry();
    resource.releaseRegistrar = new PipelineReleaseRegistrar();
    resource.workerAvailability = mock(PipelineWorkerAvailability.class);
    resource.payloadCodec = payloadCodec;
    resource.secretResolver = new LocalControlPlaneSecretResolver();
    return resource;
  }

  private void table(String name, String partitionKey, String sortKey) {
    CreateTableRequest.Builder request = CreateTableRequest.builder()
        .tableName(name)
        .keySchema(KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build())
        .attributeDefinitions(AttributeDefinition.builder().attributeName(partitionKey)
            .attributeType(ScalarAttributeType.S).build())
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(5L)
            .writeCapacityUnits(5L).build());
    if (sortKey != null) {
      request.keySchema(
          KeySchemaElement.builder().attributeName(partitionKey).keyType(KeyType.HASH).build(),
          KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());
      request.attributeDefinitions(
          AttributeDefinition.builder().attributeName(partitionKey).attributeType(ScalarAttributeType.S).build(),
          AttributeDefinition.builder().attributeName(sortKey).attributeType(ScalarAttributeType.S).build());
    }
    dynamo.createTable(request.build());
  }
}
