package org.pipelineframework.checkout.deliver_order.e2e;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.checkout.deliverorder.grpc.Orchestrator;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDeliveredSvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrderDispatchSvc;
import org.pipelineframework.checkout.deliverorder.grpc.OrchestratorServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.ProcessOrderDeliveredServiceGrpc;
import org.pipelineframework.checkout.deliverorder.grpc.ProcessOrderDispatchServiceGrpc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.PurgeQueueRequest;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueAsyncDurableHaIT {

    private static final String REGION = "eu-west-1";
    private static final GenericContainer<?> DYNAMO_DB = new GenericContainer<>("amazon/dynamodb-local:2.5.4")
        .withCommand("-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb")
        .withExposedPorts(8000)
        .waitingFor(Wait.forListeningPort());
    private static final GenericContainer<?> ELASTIC_MQ = new GenericContainer<>("softwaremill/elasticmq-native:1.6.12")
        .withExposedPorts(9324)
        .waitingFor(Wait.forListeningPort());

    private static DurableStepHarness durableStepHarness;
    private static Path logDirectory;
    private static int harnessGrpcPort;
    private static int harnessHealthPort;

    private DurableEnvironment environment;
    private OrchestratorProcess orchestratorA;
    private OrchestratorProcess orchestratorB;

    @BeforeAll
    static void beforeAll() throws Exception {
        DYNAMO_DB.start();
        ELASTIC_MQ.start();
        logDirectory = Path.of("target", "failsafe-reports", "queue-async-ha");
        Files.createDirectories(logDirectory);
        harnessGrpcPort = findFreePort();
        harnessHealthPort = findFreePort();
        durableStepHarness = new DurableStepHarness(harnessGrpcPort, harnessHealthPort);
        durableStepHarness.start();
    }

    @AfterAll
    static void afterAll() throws Exception {
        if (durableStepHarness != null) {
            durableStepHarness.close();
        }
        ELASTIC_MQ.stop();
        DYNAMO_DB.stop();
    }

    @AfterEach
    void afterEach() throws Exception {
        if (orchestratorA != null) {
            orchestratorA.close();
        }
        if (orchestratorB != null) {
            orchestratorB.close();
        }
        if (environment != null) {
            environment.close();
        }
        durableStepHarness.reset();
    }

    @Test
    void duplicateSubmitUsesSharedDurableExecutionKey() throws Exception {
        environment = DurableEnvironment.create("duplicate-submit");
        orchestratorA = environment.startProcess("a", 8182, 9192);
        orchestratorB = environment.startProcess("b", 8183, 9193);

        ReadyOrderSeed order = ReadyOrderSeed.create("duplicate");
        Orchestrator.RunAsyncResponse first = orchestratorA.submit(order, "dup-key");
        Orchestrator.RunAsyncResponse duplicate = orchestratorB.submit(order, "dup-key");

        assertFalse(first.getDuplicate(), "first submission should not be flagged as duplicate");
        assertTrue(duplicate.getDuplicate(), "second submission should be flagged as duplicate");
        assertEquals(first.getExecutionId(), duplicate.getExecutionId(), "duplicate submit must reuse execution id");

        Orchestrator.GetExecutionStatusResponse status = awaitTerminalStatus(orchestratorB, first.getExecutionId(), "SUCCEEDED");
        assertEquals("SUCCEEDED", status.getStatus());
        Orchestrator.GetExecutionResultResponse result = awaitSuccessfulResult(orchestratorA, first.getExecutionId());
        assertEquals(order.orderId(), result.getItems(0).getOrderId());
        assertEquals(1, environment.executionRowCount(), "execution table should contain one durable execution row");
    }

    @Test
    void workerKillTransfersDurableExecutionToSecondInstance() throws Exception {
        environment = DurableEnvironment.create("worker-kill");
        orchestratorA = environment.startProcess("a", 8282, 9292);

        ReadyOrderSeed order = ReadyOrderSeed.create("slow-dispatch-once");
        Orchestrator.RunAsyncResponse accepted = orchestratorA.submit(order, "kill-key");

        Awaitility.await()
            .atMost(Duration.ofSeconds(5))
            .pollInterval(Duration.ofMillis(200))
            .untilAsserted(() -> {
                Orchestrator.GetExecutionStatusResponse status = orchestratorA.status(accepted.getExecutionId());
                assertEquals("RUNNING", status.getStatus(), "execution should be running before the first worker is killed");
                assertTrue(environment.executionLeaseOwner(accepted.getExecutionId()).isPresent(),
                    "a durable lease owner should be recorded before the first worker is killed");
            });

        orchestratorA.destroy();
        orchestratorB = environment.startProcess("b", 8283, 9293);

        Orchestrator.GetExecutionStatusResponse status = awaitTerminalStatus(orchestratorB, accepted.getExecutionId(), "SUCCEEDED");
        assertEquals("SUCCEEDED", status.getStatus());
        Orchestrator.GetExecutionResultResponse result = awaitSuccessfulResult(orchestratorB, accepted.getExecutionId());
        assertEquals(order.orderId(), result.getItems(0).getOrderId());
    }

    @Test
    void sweeperRecoversMissingInitialQueueEnqueue() throws Exception {
        environment = DurableEnvironment.create("sweeper-recovery");
        orchestratorA = environment.startProcess("a", 8382, 9392, Duration.ofSeconds(5));
        orchestratorB = environment.startProcess("b", 8383, 9393, Duration.ofSeconds(5));

        ReadyOrderSeed order = ReadyOrderSeed.create("sweeper");
        Orchestrator.RunAsyncResponse accepted = orchestratorA.submit(order, "sweeper-key");
        environment.purgeWorkQueue();
        assertEquals(0, environment.workQueueMessages().size(), "purge should remove the initial queue dispatch");

        Orchestrator.GetExecutionStatusResponse status = awaitTerminalStatus(orchestratorA, accepted.getExecutionId(), "SUCCEEDED");
        assertEquals("SUCCEEDED", status.getStatus());
        Orchestrator.GetExecutionResultResponse result = awaitSuccessfulResult(orchestratorB, accepted.getExecutionId());
        assertEquals(order.orderId(), result.getItems(0).getOrderId());
    }

    @Test
    void retryExhaustionPublishesTerminalFailureToDurableDlq() throws Exception {
        environment = DurableEnvironment.create("retry-dlq");
        orchestratorA = environment.startProcess("a", 8482, 9492);
        orchestratorB = environment.startProcess("b", 8483, 9493);

        ReadyOrderSeed order = ReadyOrderSeed.create("always-fail-dispatch");
        Orchestrator.RunAsyncResponse accepted = orchestratorA.submit(order, "dlq-key");

        Orchestrator.GetExecutionStatusResponse status = awaitTerminalStatus(orchestratorB, accepted.getExecutionId(), "FAILED");
        assertEquals("FAILED", status.getStatus());
        assertTrue(status.getAttempt() >= 2, "retry exhaustion should report at least two observed retry attempts");

        List<Message> dlqMessages = Awaitility.await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(250))
            .until(() -> environment.dlqMessages(),
                messages -> !messages.isEmpty());
        String body = dlqMessages.get(0).body();
        assertTrue(body.contains(accepted.getExecutionId()), "DLQ envelope should include execution id");
        assertTrue(body.contains("retry_exhausted"), "DLQ envelope should record retry exhaustion");
    }

    private static Orchestrator.GetExecutionStatusResponse awaitTerminalStatus(
        OrchestratorProcess process,
        String executionId,
        String expectedStatus
    ) {
        return Awaitility.await()
            .atMost(Duration.ofSeconds(25))
            .pollInterval(Duration.ofMillis(250))
            .until(() -> process.status(executionId),
                status -> expectedStatus.equals(status.getStatus()));
    }

    private static Orchestrator.GetExecutionResultResponse awaitSuccessfulResult(
        OrchestratorProcess process,
        String executionId
    ) {
        return Awaitility.await()
            .atMost(Duration.ofSeconds(25))
            .pollInterval(Duration.ofMillis(250))
            .until(() -> process.result(executionId),
                result -> result.getItemsCount() == 1);
    }

    private record ReadyOrderSeed(
        String orderId,
        String customerId,
        String readyAt
    ) {
        static ReadyOrderSeed create(String prefix) {
            String token = prefix + "-" + UUID.randomUUID();
            return new ReadyOrderSeed(
                token,
                "customer-" + token,
                Instant.parse("2026-03-11T10:15:30Z").toString());
        }

        OrderDispatchSvc.ReadyOrder toProto() {
            return OrderDispatchSvc.ReadyOrder.newBuilder()
                .setOrderId(orderId)
                .setCustomerId(customerId)
                .setReadyAt(readyAt)
                .build();
        }
    }

    private static final class DurableEnvironment implements AutoCloseable {

        private final String name;
        private final String executionTable;
        private final String executionKeyTable;
        private final String workQueueUrl;
        private final String dlqQueueUrl;
        private final DynamoDbClient dynamoDbClient;
        private final SqsClient sqsClient;

        private DurableEnvironment(
            String name,
            String executionTable,
            String executionKeyTable,
            String workQueueUrl,
            String dlqQueueUrl,
            DynamoDbClient dynamoDbClient,
            SqsClient sqsClient
        ) {
            this.name = name;
            this.executionTable = executionTable;
            this.executionKeyTable = executionKeyTable;
            this.workQueueUrl = workQueueUrl;
            this.dlqQueueUrl = dlqQueueUrl;
            this.dynamoDbClient = dynamoDbClient;
            this.sqsClient = sqsClient;
        }

        static DurableEnvironment create(String scenario) {
            String name = scenario + "-" + UUID.randomUUID().toString().substring(0, 8);
            DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(REGION))
                .endpointOverride(URI.create("http://" + DYNAMO_DB.getHost() + ":" + DYNAMO_DB.getMappedPort(8000)))
                .build();
            SqsClient sqsClient = SqsClient.builder()
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                .httpClientBuilder(UrlConnectionHttpClient.builder())
                .region(Region.of(REGION))
                .endpointOverride(URI.create("http://" + ELASTIC_MQ.getHost() + ":" + ELASTIC_MQ.getMappedPort(9324)))
                .build();

            String executionTable = "tpf_execution_" + name.replace('-', '_');
            String executionKeyTable = "tpf_execution_key_" + name.replace('-', '_');
            createExecutionTable(dynamoDbClient, executionTable);
            createExecutionKeyTable(dynamoDbClient, executionKeyTable);
            String workQueueUrl = createQueue(sqsClient, "work-" + name, Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "3"));
            String dlqQueueUrl = createQueue(sqsClient, "dlq-" + name, Map.of());
            purgeQuietly(sqsClient, workQueueUrl);
            purgeQuietly(sqsClient, dlqQueueUrl);

            return new DurableEnvironment(name, executionTable, executionKeyTable, workQueueUrl, dlqQueueUrl, dynamoDbClient, sqsClient);
        }

        OrchestratorProcess startProcess(String suffix, int httpPort, int grpcPort) throws Exception {
            return startProcess(suffix, httpPort, grpcPort, Duration.ZERO);
        }

        OrchestratorProcess startProcess(String suffix, int httpPort, int grpcPort, Duration pollStartDelay) throws Exception {
            return OrchestratorProcess.start(this, suffix, httpPort, grpcPort, pollStartDelay);
        }

        int executionRowCount() {
            return dynamoDbClient.scan(ScanRequest.builder().tableName(executionTable).build()).count();
        }

        Optional<String> executionLeaseOwner(String executionId) {
            GetItemResponse response = dynamoDbClient.getItem(GetItemRequest.builder()
                .tableName(executionTable)
                .key(Map.of(
                    "tenant_id", AttributeValue.builder().s("default").build(),
                    "execution_id", AttributeValue.builder().s(executionId).build()))
                .consistentRead(true)
                .build());
            if (!response.hasItem()) {
                return Optional.empty();
            }
            AttributeValue leaseOwner = response.item().get("lease_owner");
            return leaseOwner == null ? Optional.empty() : Optional.ofNullable(leaseOwner.s());
        }

        List<Message> workQueueMessages() {
            return receiveMessages(workQueueUrl);
        }

        List<Message> dlqMessages() {
            return receiveMessages(dlqQueueUrl);
        }

        void purgeWorkQueue() {
            purgeQuietly(sqsClient, workQueueUrl);
        }

        private List<Message> receiveMessages(String queueUrl) {
            return sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .visibilityTimeout(0)
                .waitTimeSeconds(0)
                .build()).messages();
        }

        @Override
        public void close() {
            try {
                purgeQuietly(sqsClient, workQueueUrl);
                purgeQuietly(sqsClient, dlqQueueUrl);
            } finally {
                sqsClient.close();
                dynamoDbClient.close();
            }
        }

        private static void createExecutionTable(DynamoDbClient dynamoDbClient, String tableName) {
            dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                    AttributeDefinition.builder().attributeName("tenant_id").attributeType("S").build(),
                    AttributeDefinition.builder().attributeName("execution_id").attributeType("S").build())
                .keySchema(
                    KeySchemaElement.builder().attributeName("tenant_id").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("execution_id").keyType(KeyType.RANGE).build())
                .build());
        }

        private static void createExecutionKeyTable(DynamoDbClient dynamoDbClient, String tableName) {
            dynamoDbClient.createTable(CreateTableRequest.builder()
                .tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(AttributeDefinition.builder()
                    .attributeName("tenant_execution_key")
                    .attributeType("S")
                    .build())
                .keySchema(KeySchemaElement.builder()
                    .attributeName("tenant_execution_key")
                    .keyType(KeyType.HASH)
                    .build())
                .build());
        }

        private static String createQueue(
            SqsClient sqsClient,
            String queueName,
            Map<QueueAttributeName, String> attributes
        ) {
            String queueUrl;
            try {
                CreateQueueResponse response = sqsClient.createQueue(CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(attributes)
                    .build());
                queueUrl = response.queueUrl();
            } catch (SqsException e) {
                GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
                queueUrl = response.queueUrl();
            }
            if (queueUrl == null || queueUrl.isBlank()) {
                GetQueueUrlResponse response = sqsClient.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build());
                queueUrl = response.queueUrl();
            }
            URI queueUri = URI.create(queueUrl);
            return "http://" + ELASTIC_MQ.getHost() + ":" + ELASTIC_MQ.getMappedPort(9324) + queueUri.getPath();
        }

        private static void purgeQuietly(SqsClient sqsClient, String queueUrl) {
            try {
                sqsClient.purgeQueue(PurgeQueueRequest.builder().queueUrl(queueUrl).build());
            } catch (SqsException ignored) {
            }
        }
    }

    private static final class OrchestratorProcess implements AutoCloseable {

        private final Process process;
        private final int httpPort;
        private final int grpcPort;
        private final ManagedChannel channel;
        private final OrchestratorServiceGrpc.OrchestratorServiceBlockingStub blockingStub;

        private OrchestratorProcess(Process process, int httpPort, int grpcPort) {
            this.process = process;
            this.httpPort = httpPort;
            this.grpcPort = grpcPort;
            this.channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcPort)
                .usePlaintext()
                .build();
            this.blockingStub = OrchestratorServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(10, TimeUnit.SECONDS);
        }

        static OrchestratorProcess start(
            DurableEnvironment environment,
            String suffix,
            int httpPort,
            int grpcPort,
            Duration pollStartDelay
        ) throws Exception {
            Files.createDirectories(logDirectory);
            Path logFile = logDirectory.resolve(environment.name + "-" + suffix + ".log");
            ProcessBuilder builder = new ProcessBuilder(
                "java",
                "--enable-preview",
                "-jar",
                "target/quarkus-app/quarkus-run.jar");
            builder.directory(Path.of(System.getProperty("user.dir")).toFile());
            builder.redirectErrorStream(true);
            builder.redirectOutput(logFile.toFile());

            Map<String, String> env = builder.environment();
            env.put("QUARKUS_HTTP_HOST", "127.0.0.1");
            env.put("QUARKUS_HTTP_PORT", Integer.toString(httpPort));
            env.put("QUARKUS_GRPC_SERVER_HOST", "127.0.0.1");
            env.put("QUARKUS_GRPC_SERVER_PORT", Integer.toString(grpcPort));
            env.put("QUARKUS_OTEL_SDK_DISABLED", "true");
            env.put("CHECKOUT_DELIVER_FORWARD_ENABLED", "false");
            env.put("PIPELINE_HEALTH_ENABLED", "false");
            env.put("PIPELINE_DEFAULTS_RETRY_LIMIT", "0");
            env.put("PIPELINE_DEFAULTS_RETRY_WAIT_MS", "1");
            env.put("PIPELINE_ORCHESTRATOR_MODE", "QUEUE_ASYNC");
            env.put("PIPELINE_ORCHESTRATOR_STATE_PROVIDER", "dynamo");
            env.put("PIPELINE_ORCHESTRATOR_DISPATCHER_PROVIDER", "sqs");
            env.put("PIPELINE_ORCHESTRATOR_DLQ_PROVIDER", "sqs");
            env.put("PIPELINE_ORCHESTRATOR_QUEUE_URL", environment.workQueueUrl);
            env.put("PIPELINE_ORCHESTRATOR_DLQ_URL", environment.dlqQueueUrl);
            env.put("PIPELINE_ORCHESTRATOR_IDEMPOTENCY_POLICY", "CLIENT_KEY_REQUIRED");
            env.put("PIPELINE_ORCHESTRATOR_STRICT_STARTUP", "true");
            env.put("PIPELINE_ORCHESTRATOR_LEASE_MS", "1500");
            env.put("PIPELINE_ORCHESTRATOR_SWEEP_INTERVAL", "PT1S");
            env.put("PIPELINE_ORCHESTRATOR_SWEEP_LIMIT", "20");
            env.put("PIPELINE_ORCHESTRATOR_MAX_RETRIES", "2");
            env.put("PIPELINE_ORCHESTRATOR_RETRY_DELAY", "PT1S");
            env.put("PIPELINE_ORCHESTRATOR_RETRY_MULTIPLIER", "1.0");
            env.put("PIPELINE_ORCHESTRATOR_DYNAMO_EXECUTION_TABLE", environment.executionTable);
            env.put("PIPELINE_ORCHESTRATOR_DYNAMO_EXECUTION_KEY_TABLE", environment.executionKeyTable);
            env.put("PIPELINE_ORCHESTRATOR_DYNAMO_REGION", REGION);
            env.put("PIPELINE_ORCHESTRATOR_DYNAMO_ENDPOINT_OVERRIDE",
                "http://" + DYNAMO_DB.getHost() + ":" + DYNAMO_DB.getMappedPort(8000));
            env.put("AWS_ACCESS_KEY_ID", "test");
            env.put("AWS_SECRET_ACCESS_KEY", "test");
            env.put("PIPELINE_ORCHESTRATOR_SQS_REGION", REGION);
            env.put("PIPELINE_ORCHESTRATOR_SQS_ENDPOINT_OVERRIDE",
                "http://" + ELASTIC_MQ.getHost() + ":" + ELASTIC_MQ.getMappedPort(9324));
            env.put("PIPELINE_ORCHESTRATOR_SQS_LOCAL_LOOPBACK", "false");
            env.put("PIPELINE_ORCHESTRATOR_SQS_POLL_START_DELAY", pollStartDelay.toString());
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DISPATCH_HOST", "127.0.0.1");
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DISPATCH_PORT", Integer.toString(harnessGrpcPort));
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DISPATCH_PLAIN_TEXT", "true");
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DELIVERED_HOST", "127.0.0.1");
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DELIVERED_PORT", Integer.toString(harnessGrpcPort));
            env.put("QUARKUS_GRPC_CLIENTS_PROCESS_ORDER_DELIVERED_PLAIN_TEXT", "true");
            Process process = builder.start();
            waitForHealth(httpPort);
            return new OrchestratorProcess(process, httpPort, grpcPort);
        }

        Orchestrator.RunAsyncResponse submit(ReadyOrderSeed order, String idempotencyKey) {
            return blockingStub.runAsync(Orchestrator.RunAsyncRequest.newBuilder()
                .setInput(order.toProto())
                .setTenantId("default")
                .setIdempotencyKey(idempotencyKey)
                .build());
        }

        Orchestrator.GetExecutionStatusResponse status(String executionId) {
            return blockingStub.getExecutionStatus(Orchestrator.GetExecutionStatusRequest.newBuilder()
                .setTenantId("default")
                .setExecutionId(executionId)
                .build());
        }

        Orchestrator.GetExecutionResultResponse result(String executionId) {
            return blockingStub.getExecutionResult(Orchestrator.GetExecutionResultRequest.newBuilder()
                .setTenantId("default")
                .setExecutionId(executionId)
                .build());
        }

        void destroy() throws InterruptedException {
            process.destroyForcibly();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Failed to terminate orchestrator process pid="
                        + process.pid() + ", exitCode=" + exitCodeForDiagnostics());
                }
            }
        }

        @Override
        public void close() throws Exception {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            if (!process.isAlive()) {
                return;
            }
            process.destroy();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Failed to close orchestrator process pid="
                        + process.pid() + ", exitCode=" + exitCodeForDiagnostics());
                }
            }
        }

        private String exitCodeForDiagnostics() {
            try {
                return Integer.toString(process.exitValue());
            } catch (IllegalThreadStateException ignored) {
                return "unknown";
            }
        }

        private static void waitForHealth(int httpPort) {
            HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();
            URI livenessUri = URI.create("http://127.0.0.1:" + httpPort + "/q/health/live");
            Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(250))
                .ignoreExceptions()
                .until(() -> {
                    HttpRequest request = HttpRequest.newBuilder(livenessUri)
                        .timeout(Duration.ofSeconds(2))
                        .GET()
                        .build();
                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 200) {
                        return false;
                    }
                    @SuppressWarnings("unchecked")
                    Map<String, Object> body = PipelineJson.mapper().readValue(response.body(), Map.class);
                    return "UP".equals(body.get("status"));
                });
        }
    }

    private static final class DurableStepHarness implements AutoCloseable {

        private static final Duration DISPATCH_DELAY = Duration.ofSeconds(8);
        private static final long DELIVERY_OFFSET_SECONDS = 120L;

        private final int grpcPort;
        private final int healthPort;
        private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();
        private Server grpcServer;
        private HttpServer healthServer;

        private DurableStepHarness(int grpcPort, int healthPort) {
            this.grpcPort = grpcPort;
            this.healthPort = healthPort;
        }

        void start() throws IOException {
            grpcServer = NettyServerBuilder.forPort(grpcPort)
                .addService(new DispatchService())
                .addService(new DeliveredService())
                .build()
                .start();
            healthServer = HttpServer.create(new InetSocketAddress(healthPort), 0);
            healthServer.createContext("/q/health", exchange -> {
                byte[] body = "{\"status\":\"UP\"}".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
                exchange.close();
            });
            healthServer.start();
        }

        void reset() {
            attempts.clear();
        }

        @Override
        public void close() throws Exception {
            if (healthServer != null) {
                healthServer.stop(0);
            }
            if (grpcServer != null) {
                grpcServer.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        private final class DispatchService extends ProcessOrderDispatchServiceGrpc.ProcessOrderDispatchServiceImplBase {
            @Override
            public void remoteProcess(
                OrderDispatchSvc.ReadyOrder request,
                StreamObserver<OrderDispatchSvc.DispatchedOrder> responseObserver
            ) {
                try {
                    int attempt = attempts.computeIfAbsent(request.getOrderId(), ignored -> new AtomicInteger()).incrementAndGet();
                    if (request.getOrderId().startsWith("always-fail-dispatch-")) {
                        throw new IllegalStateException("forced dispatch failure for " + request.getOrderId());
                    }
                    if (request.getOrderId().startsWith("slow-dispatch-once-") && attempt == 1) {
                        sleep(DISPATCH_DELAY);
                    }
                    Instant dispatchedAt = Instant.parse(request.getReadyAt()).plusSeconds(30);
                    responseObserver.onNext(OrderDispatchSvc.DispatchedOrder.newBuilder()
                        .setOrderId(request.getOrderId())
                        .setCustomerId(request.getCustomerId())
                        .setReadyAt(request.getReadyAt())
                        .setDispatchId(UUID.nameUUIDFromBytes(request.getOrderId().getBytes(StandardCharsets.UTF_8)).toString())
                        .setDispatchedAt(dispatchedAt.toString())
                        .build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
                }
            }
        }

        private static final class DeliveredService extends ProcessOrderDeliveredServiceGrpc.ProcessOrderDeliveredServiceImplBase {
            @Override
            public void remoteProcess(
                OrderDispatchSvc.DispatchedOrder request,
                StreamObserver<OrderDeliveredSvc.DeliveredOrder> responseObserver
            ) {
                try {
                    responseObserver.onNext(OrderDeliveredSvc.DeliveredOrder.newBuilder()
                        .setOrderId(request.getOrderId())
                        .setCustomerId(request.getCustomerId())
                        .setReadyAt(request.getReadyAt())
                        .setDispatchId(request.getDispatchId())
                        .setDispatchedAt(request.getDispatchedAt())
                        .setDeliveredAt(Instant.parse(request.getDispatchedAt()).plusSeconds(DELIVERY_OFFSET_SECONDS).toString())
                        .build());
                    responseObserver.onCompleted();
                } catch (RuntimeException e) {
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).withCause(e).asRuntimeException());
                }
            }
        }

        private static void sleep(Duration duration) {
            try {
                Thread.sleep(duration.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while simulating slow dispatch.", e);
            }
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }
}
