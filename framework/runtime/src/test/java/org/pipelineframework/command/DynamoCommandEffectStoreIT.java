package org.pipelineframework.command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.CommandMachineConfirmation;
import org.pipelineframework.connector.CommandReference;
import org.pipelineframework.connector.CommandReferencePurpose;
import org.pipelineframework.connector.ConnectionRef;
import org.pipelineframework.connector.ConnectorConfigurationSnapshot;
import org.pipelineframework.connector.ConnectorOperationIdentity;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.ConnectorProviderId;
import org.pipelineframework.execution.PipelineExecutionContext;
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

@Testcontainers(disabledWithoutDocker = true)
class DynamoCommandEffectStoreIT {
    @Container
    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:3.8"))
        .withServices(LocalStackContainer.Service.DYNAMODB);

    private static DynamoDbClient dynamo;
    private String tableName;
    private DynamoCommandEffectStore store;

    @BeforeAll
    static void startClient() {
        dynamo = DynamoDbClient.builder()
            .endpointOverride(URI.create(
                LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.DYNAMODB).toString()))
            .region(Region.of(LOCALSTACK.getRegion()))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
                LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
            .build();
    }

    @AfterAll
    static void closeClient() {
        if (dynamo != null) {
            dynamo.close();
        }
    }

    @BeforeEach
    void createTable() {
        tableName = "command-effect-" + UUID.randomUUID();
        dynamo.createTable(CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName(DynamoCommandEffectStore.COMMAND_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(DynamoCommandEffectStore.REVISION)
                    .attributeType(ScalarAttributeType.N)
                    .build())
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName(DynamoCommandEffectStore.COMMAND_KEY)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(DynamoCommandEffectStore.REVISION)
                    .keyType(KeyType.RANGE)
                    .build())
            .provisionedThroughput(ProvisionedThroughput.builder()
                .readCapacityUnits(10L)
                .writeCapacityUnits(10L)
                .build())
            .build());
        dynamo.waiter().waitUntilTableExists(request -> request.tableName(tableName));
        store = new DynamoCommandEffectStore(dynamo, tableName);
    }

    @Test
    void freshStoreReplaysTypedSuccessAndNativeOutcome() {
        CommandRequest<TestInput> request = request("success", "attempt-1", "execution-1");
        CommandOutcomeSnapshot outcome = outcome(CommandEffectStatus.SUCCEEDED, "created");
        store.createPending(request, 10L).await().atMost(Duration.ofSeconds(5));
        store.markDispatching("tenant-a", request.commandId(), request.attemptId(), 20L)
            .await().atMost(Duration.ofSeconds(5));
        store.markSucceeded(
                "tenant-a",
                request.commandId(),
                request.attemptId(),
                new TestOutput("provider-9", true),
                outcome,
                30L)
            .await().atMost(Duration.ofSeconds(5));

        CommandEffectRecord restarted = new DynamoCommandEffectStore(dynamo, tableName)
            .find("tenant-a", request.commandId())
            .await().atMost(Duration.ofSeconds(5))
            .orElseThrow();

        assertEquals(CommandEffectStatus.SUCCEEDED, restarted.status());
        assertEquals(new TestInput("success", 7), restarted.input());
        assertEquals(new TestOutput("provider-9", true), restarted.output());
        assertInstanceOf(TestOutput.class, restarted.output());
        assertEquals(outcome, restarted.outcome().orElseThrow());
        assertEquals(List.of("attempt-1"), restarted.attempts().stream()
            .map(CommandEffectAttemptRecord::attemptId)
            .toList());
    }

    @Test
    void everyRetainedNonSuccessStateSurvivesRestart() {
        List<CommandEffectStatus> statuses = List.of(
            CommandEffectStatus.PENDING,
            CommandEffectStatus.DISPATCHING,
            CommandEffectStatus.FAILED_RETRYABLE,
            CommandEffectStatus.AMBIGUOUS,
            CommandEffectStatus.USER_ACTION_REQUIRED,
            CommandEffectStatus.DLQ,
            CommandEffectStatus.FAILED);

        for (CommandEffectStatus status : statuses) {
            String suffix = status.name().toLowerCase();
            CommandRequest<TestInput> request = request(suffix, "attempt-" + suffix, "execution-" + suffix);
            store.createPending(request, 10L).await().atMost(Duration.ofSeconds(5));
            if (status != CommandEffectStatus.PENDING) {
                store.markDispatching("tenant-a", request.commandId(), request.attemptId(), 20L)
                    .await().atMost(Duration.ofSeconds(5));
            }
            switch (status) {
                case FAILED_RETRYABLE -> store.markFailed(
                        "tenant-a", request.commandId(), request.attemptId(), new IllegalStateException("retry"), 30L)
                    .await().atMost(Duration.ofSeconds(5));
                case AMBIGUOUS, USER_ACTION_REQUIRED, FAILED -> store.markOutcome(
                        "tenant-a",
                        request.commandId(),
                        request.attemptId(),
                        status,
                        new IllegalStateException("outcome-" + suffix),
                        outcome(status, suffix),
                        30L)
                    .await().atMost(Duration.ofSeconds(5));
                case DLQ -> store.markDlq(
                        "tenant-a", request.commandId(), request.attemptId(), new IllegalStateException("terminal"), 30L)
                    .await().atMost(Duration.ofSeconds(5));
                default -> {
                    // PENDING and DISPATCHING are already in the requested retained state.
                }
            }

            CommandEffectRecord restarted = new DynamoCommandEffectStore(dynamo, tableName)
                .find("tenant-a", request.commandId())
                .await().atMost(Duration.ofSeconds(5))
                .orElseThrow();
            assertEquals(status, restarted.status());
            assertEquals(request.attemptId(), restarted.currentAttempt().attemptId());
            assertEquals(request.executionContext().executionId(), restarted.currentAttempt().executionId());
            if (status == CommandEffectStatus.AMBIGUOUS
                || status == CommandEffectStatus.USER_ACTION_REQUIRED
                || status == CommandEffectStatus.FAILED) {
                assertEquals(suffix, restarted.outcome().orElseThrow().outcomeCode());
            }
        }
    }

    @Test
    void competingInitialAndRetryClaimsHaveOneWinner() {
        CommandRequest<TestInput> first = request("race", "attempt-1", "execution-1");
        List<Boolean> createResults = race(
            () -> store.createPending(first, 10L).await().atMost(Duration.ofSeconds(5)),
            () -> store.createPending(first, 10L).await().atMost(Duration.ofSeconds(5)));
        assertEquals(1L, createResults.stream().filter(Boolean::booleanValue).count());

        store.markDispatching("tenant-a", first.commandId(), first.attemptId(), 20L)
            .await().atMost(Duration.ofSeconds(5));
        store.markFailed(
                "tenant-a", first.commandId(), first.attemptId(), new IllegalStateException("retry"), 30L)
            .await().atMost(Duration.ofSeconds(5));

        CommandRequest<TestInput> retryA = request("race", "attempt-2a", "execution-2a");
        CommandRequest<TestInput> retryB = request("race", "attempt-2b", "execution-2b");
        List<Boolean> retryResults = race(
            () -> store.createRetryAttempt(retryA, 40L).await().atMost(Duration.ofSeconds(5)),
            () -> store.createRetryAttempt(retryB, 40L).await().atMost(Duration.ofSeconds(5)));

        assertEquals(1L, retryResults.stream().filter(Boolean::booleanValue).count());
        CommandEffectRecord restarted = new DynamoCommandEffectStore(dynamo, tableName)
            .find("tenant-a", first.commandId())
            .await().atMost(Duration.ofSeconds(5))
            .orElseThrow();
        assertEquals(CommandEffectStatus.PENDING, restarted.status());
        assertEquals(2, restarted.attempts().size());
        assertEquals(2, restarted.currentAttempt().attemptNumber());
        assertTrue(Set.of("attempt-2a", "attempt-2b").contains(restarted.currentAttempt().attemptId()));
    }

    private static List<Boolean> race(Runnable first, Runnable second) {
        CompletableFuture<Boolean> a = CompletableFuture.supplyAsync(() -> runClaim(first));
        CompletableFuture<Boolean> b = CompletableFuture.supplyAsync(() -> runClaim(second));
        return List.of(a.join(), b.join());
    }

    private static boolean runClaim(Runnable claim) {
        try {
            claim.run();
            return true;
        } catch (CommandEffectConflictException expected) {
            return false;
        }
    }

    private static CommandRequest<TestInput> request(
        String value,
        String attemptId,
        String executionId
    ) {
        CommandDescriptor descriptor = new CommandDescriptor(
            "WriteInvoice",
            "invoice.create",
            TestInput.class.getName(),
            TestOutput.class.getName(),
            "test.CommandIdGenerator",
            CommandDuplicatePolicy.RETURN_RECORDED,
            Map.of());
        return new CommandRequest<>(
            descriptor,
            "invoice-" + value,
            attemptId,
            new TestInput(value, 7),
            new PipelineExecutionContext("tenant-a", executionId, 0),
            Map.of());
    }

    private static CommandOutcomeSnapshot outcome(CommandEffectStatus status, String code) {
        return new CommandOutcomeSnapshot(
            new ConnectorOperationIdentity(
                ConnectorProviderId.of("test.provider"),
                "invoice.create",
                ConnectorOperationKind.COMMAND,
                1),
            1,
            new ConnectorConfigurationSnapshot(
                "test.config", 1, "digest", List.of(new ConnectionRef("invoice-primary"))),
            status,
            code,
            Set.of("durable"),
            CommandMachineConfirmation.PROVIDER_ACKNOWLEDGED,
            false,
            List.of(new CommandReference(
                "invoice", "provider-9", CommandReferencePurpose.RECONCILIATION)));
    }

    record TestInput(String value, int quantity) {
    }

    record TestOutput(String providerId, boolean accepted) {
    }
}
