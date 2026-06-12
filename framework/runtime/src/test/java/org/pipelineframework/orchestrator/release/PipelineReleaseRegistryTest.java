package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.LocalPipelineBundleArtifactStore;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleManifest;
import org.pipelineframework.orchestrator.PipelineBundleManifestLoader;
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
        PipelineBundleManifest manifest = manifest();
        Path jar = jar(manifest);
        Path descriptor = releaseDescriptor(jar, manifest, "sha256:" + sha256(jar));
        PipelineReleaseRegistrar registrar = registrar();

        PipelineReleaseRecord record = registrar.validate(
            "tenant-1",
            "org.example.restaurant",
            descriptor.toString(),
            1000L);

        assertEquals("sha256:bundle", record.releaseVersion());
        assertEquals("sha256:bundle", record.contractVersion());
        assertEquals("sha256:bundle", record.bundleVersionId());
        assertTrue(Files.isRegularFile(Path.of(record.primaryArtifactPath())));
        registrar.verify(record);
    }

    @Test
    void registrarRejectsMismatchedArtifactDigest() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path jar = jar(manifest);
        Path descriptor = releaseDescriptor(jar, manifest, "sha256:deadbeef");
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
        registry.activate("tenant-1", "org.example.restaurant", "sha256:bundle", 2000L)
            .await().indefinitely();

        PipelineReleaseRegistry reloaded = new FileBackedPipelineReleaseRegistry(tempDir);
        PipelineReleaseRecord active = reloaded.active("tenant-1", "org.example.restaurant")
            .await().indefinitely()
            .orElseThrow();

        assertEquals("sha256:bundle", active.releaseVersion());
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
            existing.bundleVersionId(),
            existing.bundleHash(),
            existing.primaryArtifactPath(),
            existing.primaryArtifactSizeBytes(),
            "different-checksum",
            existing.manifest(),
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

        assertTrue(!content.contains("UpdateItemRequest"), "Dynamo release registry must not use UpdateItemRequest");
        assertTrue(!content.contains(".updateItem("), "Dynamo release registry must not call updateItem");
        assertTrue(!content.contains("return null"), "Dynamo release registry must not return null");
    }

    private PipelineReleaseRegistrar registrar() {
        PipelineReleaseRegistrar registrar = new PipelineReleaseRegistrar();
        registrar.descriptorLoader = new PipelineReleaseDescriptorLoader();
        registrar.manifestLoader = new PipelineBundleManifestLoader();
        registrar.artifactStore = new LocalPipelineBundleArtifactStore(tempDir.resolve("store"), registrar.manifestLoader);
        return registrar;
    }

    private PipelineReleaseRecord releaseRecord() {
        PipelineBundleManifest manifest = manifest();
        PipelineReleaseDescriptor descriptor = descriptor("/tmp/restaurant.jar", "sha256:artifact", manifest);
        return new PipelineReleaseRecord(
            "tenant-1",
            "org.example.restaurant",
            "sha256:bundle",
            "sha256:bundle",
            PipelineReleaseStatus.REGISTERED,
            descriptor,
            "sha256:bundle",
            "bundle",
            "/tmp/restaurant.jar",
            100L,
            "artifact",
            manifest,
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
            Map.entry("bundle_version_id", avS(record.bundleVersionId())),
            Map.entry("bundle_hash", avS(record.bundleHash())),
            Map.entry("primary_artifact_path", avS(record.primaryArtifactPath())),
            Map.entry("primary_artifact_size_bytes", avN(record.primaryArtifactSizeBytes())),
            Map.entry("primary_artifact_checksum", avS(record.primaryArtifactChecksum())),
            Map.entry("descriptor_json", avS(writeJson(record.descriptor()))),
            Map.entry("manifest_json", avS(writeJson(record.manifest()))),
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

    private Path releaseDescriptor(Path jar, PipelineBundleManifest manifest, String digest) throws Exception {
        Path descriptorPath = tempDir.resolve("pipeline-release.json");
        PipelineJson.mapper().writerWithDefaultPrettyPrinter()
            .writeValue(descriptorPath.toFile(), descriptor(jar.toString(), digest, manifest));
        return descriptorPath;
    }

    private PipelineReleaseDescriptor descriptor(String uri, String digest, PipelineBundleManifest manifest) {
        return new PipelineReleaseDescriptor(
            PipelineReleaseDescriptor.CURRENT_SCHEMA_VERSION,
            manifest.pipelineId(),
            "sha256:" + manifest.bundleHash(),
            manifest.bundleVersionId(),
            List.of(new PipelineReleaseArtifactDescriptor(
                "restaurant",
                "jar",
                uri,
                digest,
                manifest.bundleVersionId(),
                manifest.bundleHash(),
                List.of("Validate"),
                List.of("rest"))));
    }

    private Path jar(PipelineBundleManifest manifest) throws Exception {
        Path jar = tempDir.resolve("restaurant.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(PipelineBundleManifest.RESOURCE_PATH));
            output.write(PipelineJson.mapper().writeValueAsBytes(manifest));
            output.closeEntry();
        }
        return jar;
    }

    private PipelineBundleManifest manifest() {
        return new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            "org.example.restaurant",
            "sha256:bundle",
            "bundle",
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
