package org.pipelineframework.orchestrator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.dto.HostedBundleRegisterRequest;

import jakarta.ws.rs.core.Response;

class HostedBundleAdminResourceTest {

    private static final String TOKEN = "admin-token";
    private static final String AUTH = "Bearer " + TOKEN;
    private static final String TENANT_ID = "tenant-1";
    private static final String PIPELINE_ID = "org.example.pipeline";

    @TempDir
    Path tempDir;

    private HostedBundleAdminResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.AdminConfig adminConfig;

    @BeforeEach
    void setUp() {
        resource = new HostedBundleAdminResource();
        config = mock(PipelineOrchestratorConfig.class);
        adminConfig = mock(PipelineOrchestratorConfig.AdminConfig.class);
        resource.orchestratorConfig = config;
        resource.bundleRegistry = new InMemoryPipelineBundleRegistry();
        PipelineBundleRegistrar registrar = new PipelineBundleRegistrar();
        registrar.manifestLoader = new PipelineBundleManifestLoader();
        registrar.artifactStore = new LocalPipelineBundleArtifactStore(tempDir.resolve("store"), registrar.manifestLoader);
        resource.bundleRegistrar = registrar;
        resource.artifactStore = registrar.artifactStore;
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        when(config.admin()).thenReturn(adminConfig);
        when(adminConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());
    }

    @Test
    void rejectsCallsWhenDisabledByDefault() {
        when(adminConfig.enabled()).thenReturn(false);

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void startupValidationRequiresExactlyOneAdminTokenWhenEnabled() {
        when(adminConfig.enabled()).thenReturn(true);
        when(adminConfig.adminToken()).thenReturn(Optional.empty());
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());

        assertThrows(IllegalStateException.class, resource::validateConfig);

        when(adminConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.of("sys:tpf.admin.token"));

        assertThrows(IllegalStateException.class, resource::validateConfig);
    }

    @Test
    void rejectsMissingOrInvalidBearerToken() {
        when(adminConfig.enabled()).thenReturn(true);

        Response missing = resource.list(TENANT_ID, PIPELINE_ID, null).await().indefinitely();
        Response invalid = resource.list(TENANT_ID, PIPELINE_ID, "Bearer wrong").await().indefinitely();

        assertEquals(401, missing.getStatus());
        assertEquals(401, invalid.getStatus());
    }

    @Test
    void registersListsGetsAndActivatesValidBundle() throws Exception {
        when(adminConfig.enabled()).thenReturn(true);
        Path jar = bundleJar(PIPELINE_ID, false, false);

        Response registeredResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(jar.toString())).await().indefinitely();

        assertEquals(200, registeredResponse.getStatus());
        PipelineBundleRecord registered = assertInstanceOf(PipelineBundleRecord.class, registeredResponse.getEntity());
        assertEquals(PipelineBundleStatus.REGISTERED, registered.status());
        assertEquals(PIPELINE_ID, registered.pipelineId());
        assertNotEquals(jar.toString(), registered.artifactPath());
        assertTrue(Files.isRegularFile(Path.of(registered.artifactPath())));

        Response listResponse = resource.list(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();
        assertEquals(200, listResponse.getStatus());
        List<?> records = assertInstanceOf(List.class, listResponse.getEntity());
        assertEquals(1, records.size());

        Response getResponse = resource.get(TENANT_ID, PIPELINE_ID, registered.bundleVersionId(), AUTH).await().indefinitely();
        assertEquals(200, getResponse.getStatus());

        Response activeMissing = resource.active(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();
        assertEquals(404, activeMissing.getStatus());

        Files.delete(jar);
        Response activateResponse = resource.activate(TENANT_ID, PIPELINE_ID, registered.bundleVersionId(), AUTH)
            .await().indefinitely();
        assertEquals(200, activateResponse.getStatus());
        PipelineBundleRecord active = assertInstanceOf(PipelineBundleRecord.class, activateResponse.getEntity());
        assertEquals(PipelineBundleStatus.ACTIVE, active.status());

        Response activeResponse = resource.active(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();
        assertEquals(200, activeResponse.getStatus());
    }

    @Test
    void registersBundleWhoseOptionalStepKindIsOmittedFromManifest() throws Exception {
        when(adminConfig.enabled()).thenReturn(true);
        Path jar = bundleJar(PIPELINE_ID, false, false, false);

        Response registeredResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(jar.toString())).await().indefinitely();

        assertEquals(200, registeredResponse.getStatus());
        PipelineBundleRecord registered = assertInstanceOf(PipelineBundleRecord.class, registeredResponse.getEntity());
        assertEquals(PIPELINE_ID, registered.pipelineId());
    }

    @Test
    void activationFailsWhenStoredArtifactIsMissing() throws Exception {
        when(adminConfig.enabled()).thenReturn(true);
        Path jar = bundleJar(PIPELINE_ID, false, false);

        Response registeredResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(jar.toString())).await().indefinitely();
        PipelineBundleRecord registered = assertInstanceOf(PipelineBundleRecord.class, registeredResponse.getEntity());
        Files.delete(Path.of(registered.artifactPath()));

        Response activateResponse = resource.activate(TENANT_ID, PIPELINE_ID, registered.bundleVersionId(), AUTH)
            .await().indefinitely();

        assertEquals(409, activateResponse.getStatus());
    }

    @Test
    void rejectsInvalidPathMalformedManifestPipelineMismatchAndHashMismatch() throws Exception {
        when(adminConfig.enabled()).thenReturn(true);

        Response missingPath = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(tempDir.resolve("missing.jar").toString())).await().indefinitely();
        assertEquals(400, missingPath.getStatus());

        Path malformed = tempDir.resolve("malformed.jar");
        writeJar(malformed, "{not-json");
        Response malformedResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(malformed.toString())).await().indefinitely();
        assertEquals(400, malformedResponse.getStatus());

        Response mismatchResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(bundleJar("org.example.other", false, false).toString())).await().indefinitely();
        assertEquals(400, mismatchResponse.getStatus());

        Response hashMismatchResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(bundleJar(PIPELINE_ID, true, false).toString())).await().indefinitely();
        assertEquals(400, hashMismatchResponse.getStatus());

        Response noStepsResponse = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedBundleRegisterRequest(bundleJar(PIPELINE_ID, false, true).toString())).await().indefinitely();
        assertEquals(400, noStepsResponse.getStatus());
    }

    private Path bundleJar(String pipelineId, boolean corruptHash, boolean emptySteps) throws Exception {
        return bundleJar(pipelineId, corruptHash, emptySteps, true);
    }

    private Path bundleJar(String pipelineId, boolean corruptHash, boolean emptySteps, boolean includeStepKind) throws Exception {
        Map<String, Object> withoutHash = canonicalContent(pipelineId, emptySteps, includeStepKind);
        String hash = sha256(PipelineJson.mapper().writeValueAsString(withoutHash));
        if (corruptHash) {
            hash = "bad" + hash.substring(3);
        }
        Map<String, Object> finalManifest = new LinkedHashMap<>();
        finalManifest.put("schemaVersion", 1);
        finalManifest.put("pipelineId", pipelineId);
        finalManifest.put("bundleVersionId", "sha256:" + hash);
        finalManifest.put("bundleHash", hash);
        finalManifest.put("platform", withoutHash.get("platform"));
        finalManifest.put("transport", withoutHash.get("transport"));
        finalManifest.put("module", withoutHash.get("module"));
        finalManifest.put("pluginHost", withoutHash.get("pluginHost"));
        finalManifest.put("runtimeLayout", withoutHash.get("runtimeLayout"));
        finalManifest.put("steps", withoutHash.get("steps"));
        finalManifest.put("capabilities", withoutHash.get("capabilities"));
        Path jar = tempDir.resolve("bundle-" + Math.abs(pipelineId.hashCode()) + "-" + corruptHash + "-" + emptySteps + ".jar");
        writeJar(jar, PipelineJson.mapper().writeValueAsString(finalManifest));
        return jar;
    }

    private Map<String, Object> canonicalContent(String pipelineId, boolean emptySteps) {
        return canonicalContent(pipelineId, emptySteps, true);
    }

    private Map<String, Object> canonicalContent(String pipelineId, boolean emptySteps, boolean includeStepKind) {
        Map<String, Object> content = new LinkedHashMap<>();
        content.put("schemaVersion", 1);
        content.put("pipelineId", pipelineId);
        content.put("platform", "COMPUTE");
        content.put("transport", "REST");
        content.put("module", "monolith-svc");
        content.put("pluginHost", false);
        content.put("runtimeLayout", "monolith");
        content.put("steps", emptySteps ? List.of() : List.of(step(includeStepKind)));
        content.put("capabilities", capabilities());
        return content;
    }

    private Map<String, Object> step(boolean includeKind) {
        Map<String, Object> step = new LinkedHashMap<>();
        step.put("index", 0);
        step.put("authoredName", "Validate");
        if (includeKind) {
            step.put("kind", "service");
        }
        step.put("cardinality", "ONE_TO_ONE");
        step.put("inputTypeId", "Input");
        step.put("outputTypeId", "Output");
        step.put("runtimeClass", "Runtime");
        step.put("clientClass", "Client");
        return step;
    }

    private Map<String, Object> capabilities() {
        Map<String, Object> capabilities = new LinkedHashMap<>();
        capabilities.put("localTransitionExecution", true);
        capabilities.put("transitionWorkerProtocols", List.of("local", "rest", "grpc", "sqs"));
        return capabilities;
    }

    private void writeJar(Path jar, String manifestJson) throws Exception {
        try (OutputStream output = java.nio.file.Files.newOutputStream(jar);
            JarOutputStream jarOutput = new JarOutputStream(output)) {
            jarOutput.putNextEntry(new JarEntry(PipelineBundleManifest.RESOURCE_PATH));
            jarOutput.write(manifestJson.getBytes(StandardCharsets.UTF_8));
            jarOutput.closeEntry();
        }
    }

    private String sha256(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
    }
}
