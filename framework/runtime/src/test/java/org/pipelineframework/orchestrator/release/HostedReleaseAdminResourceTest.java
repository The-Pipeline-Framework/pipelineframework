package org.pipelineframework.orchestrator.release;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.orchestrator.LocalControlPlaneSecretResolver;
import org.pipelineframework.orchestrator.LocalPipelineBundleArtifactStore;
import org.pipelineframework.orchestrator.PipelineBundleCapabilities;
import org.pipelineframework.orchestrator.PipelineBundleManifest;
import org.pipelineframework.orchestrator.PipelineBundleManifestLoader;
import org.pipelineframework.orchestrator.PipelineBundleStepDescriptor;
import org.pipelineframework.orchestrator.PipelineOrchestratorConfig;
import org.pipelineframework.orchestrator.release.dto.HostedReleaseRegisterRequest;

class HostedReleaseAdminResourceTest {

    private static final String TENANT_ID = "tenant-1";
    private static final String PIPELINE_ID = "org.example.restaurant";
    private static final String TOKEN = "admin-token";
    private static final String AUTH = "Bearer " + TOKEN;

    @TempDir
    Path tempDir;

    private HostedReleaseAdminResource resource;
    private PipelineOrchestratorConfig config;
    private PipelineOrchestratorConfig.AdminConfig adminConfig;

    @BeforeEach
    void setUp() {
        resource = new HostedReleaseAdminResource();
        config = mock(PipelineOrchestratorConfig.class);
        adminConfig = mock(PipelineOrchestratorConfig.AdminConfig.class);
        resource.orchestratorConfig = config;
        resource.releaseRegistry = new InMemoryPipelineReleaseRegistry();
        PipelineReleaseRegistrar registrar = new PipelineReleaseRegistrar();
        registrar.descriptorLoader = new PipelineReleaseDescriptorLoader();
        registrar.manifestLoader = new PipelineBundleManifestLoader();
        registrar.artifactStore = new LocalPipelineBundleArtifactStore(tempDir.resolve("store"), registrar.manifestLoader);
        resource.releaseRegistrar = registrar;
        resource.secretResolver = new LocalControlPlaneSecretResolver();
        when(config.admin()).thenReturn(adminConfig);
        when(adminConfig.enabled()).thenReturn(true);
        when(adminConfig.adminToken()).thenReturn(Optional.of(TOKEN));
        when(adminConfig.adminTokenRef()).thenReturn(Optional.empty());
    }

    @Test
    void registerListGetActiveAndActivateRelease() throws Exception {
        Path descriptor = releaseDescriptor();

        Response register = resource.register(
            TENANT_ID,
            PIPELINE_ID,
            AUTH,
            new HostedReleaseRegisterRequest(descriptor.toString()))
            .await().indefinitely();

        assertEquals(200, register.getStatus());
        PipelineReleaseRecord registered = assertInstanceOf(PipelineReleaseRecord.class, register.getEntity());
        assertEquals("sha256:bundle", registered.releaseVersion());

        Response list = resource.list(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();
        assertEquals(200, list.getStatus());
        assertEquals(1, assertInstanceOf(List.class, list.getEntity()).size());

        Response get = resource.get(TENANT_ID, PIPELINE_ID, registered.releaseVersion(), AUTH)
            .await().indefinitely();
        assertEquals(200, get.getStatus());

        Response activate = resource.activate(TENANT_ID, PIPELINE_ID, registered.releaseVersion(), AUTH)
            .await().indefinitely();
        assertEquals(200, activate.getStatus());

        Response active = resource.active(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();
        assertEquals(200, active.getStatus());
        PipelineReleaseRecord activeRecord = assertInstanceOf(PipelineReleaseRecord.class, active.getEntity());
        assertEquals(PipelineReleaseStatus.ACTIVE, activeRecord.status());
    }

    @Test
    void rejectsWhenDisabled() {
        when(adminConfig.enabled()).thenReturn(false);

        Response response = resource.list(TENANT_ID, PIPELINE_ID, AUTH).await().indefinitely();

        assertEquals(404, response.getStatus());
    }

    @Test
    void rejectsInvalidToken() {
        Response response = resource.list(TENANT_ID, PIPELINE_ID, "Bearer wrong").await().indefinitely();

        assertEquals(401, response.getStatus());
    }

    private Path releaseDescriptor() throws Exception {
        PipelineBundleManifest manifest = manifest();
        Path jar = tempDir.resolve("restaurant.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            output.putNextEntry(new JarEntry(PipelineBundleManifest.RESOURCE_PATH));
            output.write(PipelineJson.mapper().writeValueAsBytes(manifest));
            output.closeEntry();
        }
        Path descriptor = tempDir.resolve("pipeline-release.json");
        PipelineJson.mapper().writerWithDefaultPrettyPrinter().writeValue(descriptor.toFile(), Map.of(
            "schemaVersion", 1,
            "pipelineId", PIPELINE_ID,
            "contractVersion", "sha256:bundle",
            "releaseVersion", "sha256:bundle",
            "artifacts", List.of(Map.of(
                "artifactId", "restaurant",
                "kind", "jar",
                "uri", jar.toString(),
                "digest", "sha256:" + sha256(jar),
                "bundleVersionId", "sha256:bundle",
                "bundleHash", "bundle",
                "stepIds", List.of("Validate"),
                "capabilities", List.of("rest")))));
        return descriptor;
    }

    private static PipelineBundleManifest manifest() {
        return new PipelineBundleManifest(
            PipelineBundleManifest.CURRENT_SCHEMA_VERSION,
            PIPELINE_ID,
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

    private static String sha256(Path path) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(Files.readAllBytes(path));
        return HexFormat.of().formatHex(digest.digest());
    }
}
