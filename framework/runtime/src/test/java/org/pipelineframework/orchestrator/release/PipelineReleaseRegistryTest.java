package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.LocalPipelineReleaseArtifactStore;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsResponse;

class PipelineReleaseRegistryTest {

    @TempDir
    Path tempDir;

    @Test
    void registrarValidatesJarReleaseAndStoresExecutableArtifact() throws Exception {
        PipelineContractDescriptor contract = contract();
        Path jar = jar(contract);
        Path descriptor = releaseDescriptor(jar, contract, "sha256:" + sha256(jar));
        PipelineReleaseRegistrar registrar = registrar();

        PipelineReleaseRecord record = registrar.validate(
            "tenant-1",
            "org.example.restaurant",
            descriptor.toString(),
            1000L);

        assertEquals("sha256:contract", record.releaseVersion());
        assertEquals("sha256:contract", record.contractVersion());
        assertEquals("restaurant", record.primaryArtifactId());
        assertEquals("sha256:" + sha256(jar), record.primaryArtifactDigest());
        assertTrue(Files.isRegularFile(Path.of(URI.create(record.primaryArtifactUri()))));
        registrar.verify(record);
    }

    @Test
    void registrarRejectsMismatchedArtifactDigest() throws Exception {
        PipelineContractDescriptor contract = contract();
        Path jar = jar(contract);
        Path descriptor = releaseDescriptor(jar, contract, "sha256:deadbeef");
        PipelineReleaseRegistrar registrar = registrar();

        assertThrows(IllegalArgumentException.class, () -> registrar.validate(
            "tenant-1",
            "org.example.restaurant",
            descriptor.toString(),
            1000L));
    }

    @Test
    void fileRegistryPersistsRegisteredAndActiveRelease() throws Exception {
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new FileBackedPipelineReleaseRegistry(tempDir);

        registry.register(record).await().indefinitely();
        registry.activate("tenant-1", "org.example.restaurant", "sha256:contract", 2000L)
            .await().indefinitely();

        PipelineReleaseRegistry reloaded = new FileBackedPipelineReleaseRegistry(tempDir);
        PipelineReleaseRecord active = reloaded.active("tenant-1", "org.example.restaurant")
            .await().indefinitely()
            .orElseThrow();

        assertEquals("sha256:contract", active.releaseVersion());
        assertEquals(PipelineReleaseStatus.ACTIVE, active.status());
        assertEquals(2000L, active.activatedAtEpochMs());
    }

    @Test
    void dynamoRegistryRegistersReleaseWithConditionalPut() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new DynamoPipelineReleaseRegistry(client, dynamoConfig());
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(Map.of()).build());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(List.of()).build());
        when(client.putItem(any(PutItemRequest.class)))
            .thenReturn(PutItemResponse.builder().build());

        PipelineReleaseRecord registered = registry.register(record).await().indefinitely();

        assertEquals(record.releaseVersion(), registered.releaseVersion());
        verify(client).putItem(argThat((PutItemRequest request) ->
            "tpf_release_registry".equals(request.tableName())
                && request.conditionExpression().contains("attribute_not_exists")));
    }

    @Test
    void dynamoRegistryDuplicateRegistrationIsIdempotentWhenMetadataMatches() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new DynamoPipelineReleaseRegistry(client, dynamoConfig());
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(dynamoReleaseItem(record)).build());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(List.of()).build());

        PipelineReleaseRecord registered = registry.register(record).await().indefinitely();

        assertEquals(record.releaseVersion(), registered.releaseVersion());
        verify(client, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void dynamoRegistryRejectsDuplicateRegistrationWithDifferentMetadata() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineReleaseRecord existing = releaseRecord();
        PipelineReleaseRecord conflicting = new PipelineReleaseRecord(
            existing.tenantId(),
            existing.pipelineId(),
            existing.contractVersion(),
            existing.releaseVersion(),
            existing.status(),
            existing.descriptor(),
            existing.primaryArtifactId(),
            existing.primaryArtifactDigest(),
            existing.primaryArtifactUri(),
            existing.primaryArtifactSizeBytes(),
            "different-checksum",
            existing.contract(),
            existing.createdAtEpochMs(),
            existing.updatedAtEpochMs(),
            existing.activatedAtEpochMs());
        PipelineReleaseRegistry registry = new DynamoPipelineReleaseRegistry(client, dynamoConfig());
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(dynamoReleaseItem(existing)).build());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(List.of()).build());

        assertThrows(IllegalStateException.class, () -> registry.register(conflicting).await().indefinitely());
        verify(client, never()).putItem(any(PutItemRequest.class));
    }

    @Test
    void dynamoRegistryActivationAppendsActivationEvent() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new DynamoPipelineReleaseRegistry(client, dynamoConfig());
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(dynamoReleaseItem(record)).build());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(List.of()).build());
        when(client.transactWriteItems(any(TransactWriteItemsRequest.class)))
            .thenReturn(TransactWriteItemsResponse.builder().build());

        PipelineReleaseRecord active = registry.activate(
                record.tenantId(),
                record.pipelineId(),
                record.releaseVersion(),
                3000L)
            .await().indefinitely()
            .orElseThrow();

        assertEquals(PipelineReleaseStatus.ACTIVE, active.status());
        assertEquals(3000L, active.activatedAtEpochMs());
        verify(client).transactWriteItems(argThat((TransactWriteItemsRequest request) ->
            request.transactItems().size() == 2
                && request.transactItems().get(0).conditionCheck() != null
                && request.transactItems().get(1).put() != null
                && request.transactItems().get(1).put().conditionExpression().contains("attribute_not_exists")));
    }

    @Test
    void dynamoRegistryActiveReadsLatestActivationEvent() {
        DynamoDbClient client = mock(DynamoDbClient.class);
        PipelineReleaseRecord record = releaseRecord();
        PipelineReleaseRegistry registry = new DynamoPipelineReleaseRegistry(client, dynamoConfig());
        when(client.query(any(QueryRequest.class)))
            .thenReturn(QueryResponse.builder().items(List.of(dynamoActivationItem(record, 4000L))).build());
        when(client.getItem(any(GetItemRequest.class)))
            .thenReturn(GetItemResponse.builder().item(dynamoReleaseItem(record)).build());

        PipelineReleaseRecord active = registry.active(record.tenantId(), record.pipelineId())
            .await().indefinitely()
            .orElseThrow();

        assertEquals(record.releaseVersion(), active.releaseVersion());
        assertEquals(PipelineReleaseStatus.ACTIVE, active.status());
        assertEquals(4000L, active.activatedAtEpochMs());
        verify(client).query(argThat((QueryRequest request) ->
            Boolean.FALSE.equals(request.scanIndexForward())));
    }

    @Test
    void dynamoRegistryImplementationDoesNotUseUpdatesOrReturnNull() throws Exception {
        Path source = Path.of(System.getProperty("user.dir"))
            .resolve("src/main/java/org/pipelineframework/orchestrator/release/DynamoPipelineReleaseRegistry.java");
        String content = Files.readString(source);

        assertFalse(content.contains("UpdateItemRequest"), "Dynamo release registry must not use UpdateItemRequest");
        assertFalse(content.contains(".updateItem("), "Dynamo release registry must not call updateItem");
        assertFalse(content.contains("return null"), "Dynamo release registry must not return null");
    }

    private PipelineReleaseRegistrar registrar() {
        PipelineReleaseRegistrar registrar = new PipelineReleaseRegistrar();
        registrar.descriptorLoader = new PipelineReleaseDescriptorLoader();
        registrar.contractLoader = new PipelineContractDescriptorLoader();
        registrar.artifactStore = new LocalPipelineReleaseArtifactStore(tempDir.resolve("store"));
        return registrar;
    }

    private PipelineReleaseRecord releaseRecord() {
        PipelineContractDescriptor contract = contract();
        PipelineReleaseDescriptor descriptor = descriptor("/tmp/restaurant.jar", "sha256:artifact", contract);
        return new PipelineReleaseRecord(
            "tenant-1",
            "org.example.restaurant",
            "sha256:contract",
            "sha256:contract",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            "restaurant",
            "sha256:artifact",
            Path.of("/tmp/restaurant.jar").toUri().toString(),
            100L,
            "artifact",
            contract,
            1000L,
            1000L,
            0L);
    }

    private PipelineOrchestratorConfig dynamoConfig() {
        PipelineOrchestratorConfig config = mock(PipelineOrchestratorConfig.class);
        PipelineOrchestratorConfig.DynamoConfig dynamo = mock(PipelineOrchestratorConfig.DynamoConfig.class);
        when(config.dynamo()).thenReturn(dynamo);
        when(dynamo.releaseTable()).thenReturn("tpf_release_registry");
        when(dynamo.region()).thenReturn(java.util.Optional.empty());
        when(dynamo.endpointOverride()).thenReturn(java.util.Optional.empty());
        return config;
    }

    private Map<String, AttributeValue> dynamoReleaseItem(PipelineReleaseRecord record) {
        return Map.ofEntries(
            Map.entry("registry_key", avS(record.tenantId() + "#" + record.pipelineId())),
            Map.entry("registry_sort", avS("release:" + record.releaseVersion())),
            Map.entry("record_type", avS("RELEASE")),
            Map.entry("tenant_id", avS(record.tenantId())),
            Map.entry("pipeline_id", avS(record.pipelineId())),
            Map.entry("contract_version", avS(record.contractVersion())),
            Map.entry("release_version", avS(record.releaseVersion())),
            Map.entry("primary_artifact_id", avS(record.primaryArtifactId())),
            Map.entry("primary_artifact_digest", avS(record.primaryArtifactDigest())),
            Map.entry("primary_artifact_uri", avS(record.primaryArtifactUri())),
            Map.entry("primary_artifact_size_bytes", avN(record.primaryArtifactSizeBytes())),
            Map.entry("primary_artifact_checksum", avS(record.primaryArtifactChecksum())),
            Map.entry("descriptor_json", avS(writeJson(record.descriptor()))),
            Map.entry("contract_json", avS(writeJson(record.contract()))),
            Map.entry("created_at_epoch_ms", avN(record.createdAtEpochMs())),
            Map.entry("updated_at_epoch_ms", avN(record.updatedAtEpochMs())),
            Map.entry("activated_at_epoch_ms", avN(record.activatedAtEpochMs())));
    }

    private Map<String, AttributeValue> dynamoActivationItem(PipelineReleaseRecord record, long activatedAtEpochMs) {
        return Map.ofEntries(
            Map.entry("registry_key", avS(record.tenantId() + "#" + record.pipelineId())),
            Map.entry("registry_sort", avS("activation:0000000000000004000:" + record.releaseVersion())),
            Map.entry("record_type", avS("ACTIVATION")),
            Map.entry("tenant_id", avS(record.tenantId())),
            Map.entry("pipeline_id", avS(record.pipelineId())),
            Map.entry("release_version", avS(record.releaseVersion())),
            Map.entry("activated_at_epoch_ms", avN(activatedAtEpochMs)));
    }

    private AttributeValue avS(String value) {
        return AttributeValue.builder().s(value).build();
    }

    private AttributeValue avN(long value) {
        return AttributeValue.builder().n(Long.toString(value)).build();
    }

    private String writeJson(Object value) {
        try {
            return PipelineJson.mapper().writeValueAsString(value);
        } catch (Exception e) {
            throw new IllegalStateException("Failed serializing test value", e);
        }
    }

    private Path releaseDescriptor(Path jar, PipelineContractDescriptor contract, String digest) throws Exception {
        Path descriptorPath = tempDir.resolve("pipeline-release.json");
        PipelineJson.mapper().writerWithDefaultPrettyPrinter()
            .writeValue(descriptorPath.toFile(), descriptor(jar.toString(), digest, contract));
        return descriptorPath;
    }

    private PipelineReleaseDescriptor descriptor(String uri, String digest, PipelineContractDescriptor contract) {
        return new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            contract.pipelineId(),
            contract.contractVersion(),
            contract.contractVersion(),
            List.of(new PipelineReleaseArtifactDescriptor(
                "restaurant",
                "jar",
                uri,
                digest,
                List.of("Validate"),
                List.of("rest"))));
    }

    private Path jar(PipelineContractDescriptor contract) throws Exception {
        Path jar = tempDir.resolve("restaurant.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(PipelineContractDescriptor.RESOURCE_PATH));
            output.write(PipelineJson.mapper().writeValueAsBytes(contract));
            output.closeEntry();
        }
        return jar;
    }

    private PipelineContractDescriptor contract() {
        return new PipelineContractDescriptor(
            PipelineContractDescriptor.CURRENT_SCHEMA_VERSION,
            "org.example.restaurant",
            "sha256:contract",
            "contract",
            "COMPUTE",
            "REST",
            "monolith-svc",
            false,
            "monolith",
            List.of(new PipelineBundleStepDescriptor(
                0,
                "Validate",
                "service",
                "ONE_TO_ONE",
                String.class.getName(),
                "Output",
                "Runtime",
                "Client",
                null)),
            PipelineBundleCapabilities.defaults());
    }

    private String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(Files.readAllBytes(path));
        return HexFormat.of().formatHex(digest.digest());
    }
}
