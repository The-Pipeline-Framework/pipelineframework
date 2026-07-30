package org.pipelineframework.awaitable.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
import org.testcontainers.utility.DockerImageName;

/** Real Kafka envelope parsing and admission into a fresh Dynamo await store. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaAwaitCompletionDynamoIT {
  private static final String PREFIX = "kafka_anchor";

  @Container
  static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
      .withServices(LocalStackContainer.Service.DYNAMODB);

  private DynamoDbClient dynamo;

  @BeforeAll
  void setUp() {
    dynamo = DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
        .region(Region.of(LOCALSTACK.getRegion()))
        .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .build();
    table(PREFIX + "_interaction", "tenant_id", "interaction_id");
    table(PREFIX + "_interaction_key", "lookup_key", null);
  }

  @Test
  void kafkaCompletionEnvelopeIsAdmittedIntoDynamoAfterFreshStoreReconstruction() throws Exception {
    long now = System.currentTimeMillis();
    AwaitInteractionStore creator = DynamoAwaitLifecycleTestStores.interactionStore(dynamo, PREFIX);
    AwaitInteractionRecord pending = creator.createOrGet(new AwaitCreateCommand(
            "tenant-kafka", "execution-kafka", "AwaitDecision", 2, String.class.getName(),
            "cause-kafka", "idem-request-kafka", "corr-kafka", Map.of("request", "value"),
            null, null, "kafka", "unit-kafka", null, now, now + 60_000L, now / 1_000L + 3_600L))
        .await().indefinitely().record();

    AwaitInteractionStore admissionStore = DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, PREFIX);
    PipelineExecutionService executionService = mock(PipelineExecutionService.class);
    when(executionService.completeAwaitInteraction(any(AwaitCompletionCommand.class)))
        .thenAnswer(invocation -> admissionStore.complete(invocation.getArgument(0)));
    KafkaAwaitCompletionConsumer consumer = new KafkaAwaitCompletionConsumer(executionService);
    String body = PipelineJson.mapper().writeValueAsString(new KafkaAwaitCompletionEnvelope(
        pending.tenantId(), pending.interactionId(), pending.correlationId(), null,
        "idem-completion-kafka", Map.of("decision", "approved"), "provider"));
    AtomicBoolean acknowledged = new AtomicBoolean();
    consumer.consume(Message.of(body, () -> {
      acknowledged.set(true);
      return CompletableFuture.completedFuture(null);
    }, ignored -> CompletableFuture.completedFuture(null))).toCompletableFuture().get(10, TimeUnit.SECONDS);

    AwaitInteractionRecord restored = DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, PREFIX)
        .get(pending.tenantId(), pending.interactionId()).await().indefinitely().orElseThrow();
    assertTrue(restored.responsePayload() instanceof java.util.Map);
    assertEquals(AwaitInteractionStatus.COMPLETED, restored.status());
    assertEquals(Map.of("decision", "approved"), restored.responsePayload());
    assertEquals(true, acknowledged.get());
  }

  private void table(String name, String hash, String range) {
    CreateTableRequest.Builder builder = CreateTableRequest.builder().tableName(name)
        .attributeDefinitions(range == null
            ? java.util.List.of(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build())
            : java.util.List.of(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build(), AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build()))
        .keySchema(range == null
            ? java.util.List.of(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build())
            : java.util.List.of(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build(), KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build()))
        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build());
    dynamo.createTable(builder.build());
    dynamo.waiter().waitUntilTableExists(request -> request.tableName(name));
  }
}
