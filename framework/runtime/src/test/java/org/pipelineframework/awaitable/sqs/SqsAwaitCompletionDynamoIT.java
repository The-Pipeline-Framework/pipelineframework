package org.pipelineframework.awaitable.sqs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.pipelineframework.PipelineExecutionService;
import org.pipelineframework.awaitable.AwaitCompletionCommand;
import org.pipelineframework.awaitable.AwaitCreateCommand;
import org.pipelineframework.awaitable.AwaitInteractionRecord;
import org.pipelineframework.awaitable.AwaitInteractionStatus;
import org.pipelineframework.awaitable.spi.AwaitInteractionStore;
import org.pipelineframework.awaitable.store.DynamoAwaitLifecycleTestStores;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
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
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

/** Real SQS poller envelope parsing, admission, and acknowledgement over a Dynamo interaction. */
@Testcontainers(disabledWithoutDocker = true)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqsAwaitCompletionDynamoIT {
  private static final String PREFIX = "sqs_anchor";
  @Container static final LocalStackContainer LOCALSTACK = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices(LocalStackContainer.Service.DYNAMODB);
  private DynamoDbClient dynamo;

  @BeforeAll void setUp() {
    dynamo = DynamoDbClient.builder().endpointOverride(URI.create(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString())).region(Region.of(LOCALSTACK.getRegion())).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey()))).build();
    table(PREFIX + "_interaction", "tenant_id", "interaction_id"); table(PREFIX + "_interaction_key", "lookup_key", null);
  }

  @Test void sqsCompletionEnvelopeIsAdmittedAndDeletedAfterDynamoPersistence() throws Exception {
    long now = System.currentTimeMillis();
    AwaitInteractionStore creator = DynamoAwaitLifecycleTestStores.interactionStore(dynamo, PREFIX);
    AwaitInteractionRecord pending = creator.createOrGet(new AwaitCreateCommand("tenant-sqs", "execution-sqs", "AwaitDecision", 2, String.class.getName(), "cause", "request-idem", "corr-sqs", Map.of("request", "value"), null, null, "sqs", "unit-sqs", null, now, now + 60_000, now / 1_000 + 3_600)).await().indefinitely().record();
    AwaitInteractionStore admissionStore = DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, PREFIX);
    PipelineExecutionService service = mock(PipelineExecutionService.class);
    when(service.completeAwaitInteraction(any(AwaitCompletionCommand.class))).thenAnswer(invocation -> admissionStore.complete(invocation.getArgument(0)));
    SqsClient client = mock(SqsClient.class);
    String body = PipelineJson.mapper().writeValueAsString(new SqsAwaitCompletionEnvelope(pending.tenantId(), pending.interactionId(), pending.correlationId(), null, "completion-idem", Map.of("decision", "approved"), "provider"));
    when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(ReceiveMessageResponse.builder().messages(Message.builder().messageId("message-1").receiptHandle("receipt-1").body(body).build()).build());
    SqsAwaitCompletionPoller poller = new SqsAwaitCompletionPoller(mock(PipelineOrchestratorConfig.class), service, client);
    try {
      poller.pollOnce(new SqsAwaitCompletionPoller.SqsAwaitPollerConfig(true, Optional.of("http://sqs.local/responses"), Duration.ZERO, Duration.ofSeconds(10), Duration.ofSeconds(5), 1, 1)).await().atMost(Duration.ofSeconds(10));
    } finally { poller.shutdown(); }
    AwaitInteractionRecord restored = DynamoAwaitLifecycleTestStores.interactionStoreForCompletion(dynamo, PREFIX).get(pending.tenantId(), pending.interactionId()).await().indefinitely().orElseThrow();
    assertEquals(AwaitInteractionStatus.COMPLETED, restored.status()); assertEquals(Map.of("decision", "approved"), restored.responsePayload());
  }

  private void table(String name, String hash, String range) {
    CreateTableRequest.Builder b = CreateTableRequest.builder().tableName(name).attributeDefinitions(range == null ? List.of(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build()) : List.of(AttributeDefinition.builder().attributeName(hash).attributeType(ScalarAttributeType.S).build(), AttributeDefinition.builder().attributeName(range).attributeType(ScalarAttributeType.S).build())).keySchema(range == null ? List.of(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build()) : List.of(KeySchemaElement.builder().attributeName(hash).keyType(KeyType.HASH).build(), KeySchemaElement.builder().attributeName(range).keyType(KeyType.RANGE).build())).provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(10L).writeCapacityUnits(10L).build());
    dynamo.createTable(b.build()); dynamo.waiter().waitUntilTableExists(r -> r.tableName(name));
  }
}
