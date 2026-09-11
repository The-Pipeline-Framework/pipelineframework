package org.pipelineframework.examples.openapiproof;

import static org.junit.jupiter.api.Assertions.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.junit.jupiter.api.Test;
import org.pipelineframework.connector.*;
import org.pipelineframework.examples.agentproof.domain.StartJobRequest;

class JobProofContractTest {
    private static final String KEY = "host-owned-unit-test-signing-key";

    @Test
    void successfulAcknowledgementRequiresAcceptance() throws Exception {
        var mapper = org.pipelineframework.config.pipeline.PipelineJson.mapper();
        assertTrue(mapper.readValue("{\"jobId\":\"j1\",\"accepted\":true}",
            org.pipelineframework.examples.agentproof.domain.JobAccepted.class).accepted());
        assertThrows(com.fasterxml.jackson.core.JsonProcessingException.class,
            () -> mapper.readValue("{\"jobId\":\"j1\",\"accepted\":false}",
                org.pipelineframework.examples.agentproof.domain.JobAccepted.class));
    }

    @Test
    void canonicalInputAndApplicationSourcesContainOnlyTheirOwnedBoundaries() throws Exception {
        assertEquals(List.of("jobId", "value"), Arrays.stream(StartJobRequest.class.getRecordComponents())
            .map(java.lang.reflect.RecordComponent::getName).toList());
        try (var paths = Files.walk(Path.of("src/main/java"))) {
            var sources = paths.filter(Files::isRegularFile).toList();
            assertEquals(List.of("JobAccepted.java", "JobCallback.java", "JobCallbackAuthenticator.java",
                "JobCallbackEndpointResolver.java", "JobCommandIdGenerator.java", "JobCompletionProjector.java",
                "JobConnectionResolver.java", "JobResult.java", "StartJobRequest.java"), sources.stream()
                .map(path -> path.getFileName().toString()).sorted().toList());
            for (Path source : sources) {
                String text = Files.readString(source);
                for (String forbidden : List.of("@Path(", "implements ConnectorProvider", "AwaitInteractionStore",
                    "OpenAPIV3Parser", "PipelineExecutionService", "connector-command")) {
                    assertFalse(text.contains(forbidden), source + " introduces " + forbidden);
                }
            }
        }
    }

    @Test
    void signaturePolicyRejectsWrongTenantCallbackAndAlteredBody() throws Exception {
        var auth = new JobCallbackAuthenticator(Optional.of(KEY), "default");
        byte[] body = "{\"jobId\":\"j1\",\"status\":\"completed\"}".getBytes(StandardCharsets.UTF_8);
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(KEY.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        String signature = HexFormat.of().formatHex(mac.doFinal(body));
        assertTrue(auth.authenticate(request("default", "job.completed", body, signature))
            .toCompletableFuture().get().isPresent());
        assertTrue(auth.authenticate(request("other-tenant", "job.completed", body, signature))
            .toCompletableFuture().get().isEmpty());
        assertTrue(auth.authenticate(request("default", "another.callback", body, signature))
            .toCompletableFuture().get().isEmpty());
        assertTrue(auth.authenticate(request("default", "job.completed", "{}".getBytes(StandardCharsets.UTF_8), signature))
            .toCompletableFuture().get().isEmpty());
    }

    @Test
    void executionBranchingAndReplayDescribeOneNativeCommandNode() throws Exception {
        Path metadata = Path.of("target/classes/META-INF/pipeline");
        var mapper = org.pipelineframework.config.pipeline.PipelineJson.mapper();
        var order = mapper.readTree(Files.readString(metadata.resolve("order.json")));
        var branching = mapper.readTree(Files.readString(metadata.resolve("branching.json")));
        var replay = mapper.readTree(Files.readString(metadata.resolve("replay-topology.json")));
        assertEquals(1, order.required("order").size());
        assertEquals(1, branching.required("steps").size());
        assertEquals(StartJobRequest.class.getName(), branching.required("steps").get(0)
            .required("inputRuntimeClass").textValue());
        assertEquals(1, replay.required("steps").size());
        assertEquals("command", replay.required("steps").get(0).required("renderRole").textValue());
        assertTrue(replay.required("steps").get(0).required("deferredCompletion").booleanValue());
        assertEquals(0, replay.required("transitions").size());
    }

    private ProviderCallbackAuthenticationRequest request(String tenant, String callback, byte[] body, String signature) {
        return new ProviderCallbackAuthenticationRequest(new ProviderCallbackRequest(tenant, "interaction",
            new ConnectorOperationIdentity(ConnectorProviderId.of("http.client"), "job.start", ConnectorOperationKind.COMMAND, 1),
            callback), "POST", Map.of("X-Callback-Signature", List.of(signature)), body,
            List.of(new ProviderCallbackAuthenticationRequest.SecurityRequirement("callbackSignature", List.of(),
                List.of(new ProviderCallbackAuthenticationRequest.SecurityTarget("HEADER", "X-Callback-Signature")))));
    }
}
