package org.pipelineframework.examples.openapiproof;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import io.quarkus.arc.Unremovable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.connector.ProviderCallbackActor;
import org.pipelineframework.connector.ProviderCallbackAuthenticationRequest;
import org.pipelineframework.connector.ProviderCallbackAuthenticator;

/** Demonstrates host-owned raw-body HMAC authentication; credentials never enter canonical input or pins. */
@ApplicationScoped
@Unremovable
public class JobCallbackAuthenticator implements ProviderCallbackAuthenticator {
    private final Optional<String> signingKey;
    private final String tenant;

    @Inject
    public JobCallbackAuthenticator(
        @ConfigProperty(name = "openapi.proof.callback-signing-key") Optional<String> signingKey,
        @ConfigProperty(name = "openapi.proof.callback-tenant", defaultValue = "default") String tenant) {
        this.signingKey = signingKey;
        this.tenant = tenant;
    }

    @Override
    public CompletionStage<Optional<ProviderCallbackActor>> authenticate(ProviderCallbackAuthenticationRequest request) {
        if (!tenant.equals(request.callback().tenantId())
            || !"job.completed".equals(request.callback().callbackId())
            || !"job.start".equals(request.callback().operation().operationId())
            || !"http.client".equals(request.callback().operation().providerId().value())
            || request.callback().operation().majorVersion() != 1
            || request.security().size() != 1
            || !"callbackSignature".equals(request.security().getFirst().scheme())
            || !request.security().getFirst().scopes().isEmpty()
            || !request.security().getFirst().targets().equals(List.of(
                new ProviderCallbackAuthenticationRequest.SecurityTarget("HEADER", "X-Callback-Signature")))) {
            return CompletableFuture.completedStage(Optional.empty());
        }
        List<String> signatures = request.headers().getOrDefault("X-Callback-Signature", List.of());
        if (signatures.size() != 1 || !signatures.getFirst().matches("[0-9a-f]{64}")) {
            return CompletableFuture.completedStage(Optional.empty());
        }
        String key = signingKey.filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalStateException("Configure the callback signing key"));
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            boolean accepted = MessageDigest.isEqual(mac.doFinal(request.body()),
                HexFormat.of().parseHex(signatures.getFirst()));
            return CompletableFuture.completedStage(accepted
                ? Optional.of(new ProviderCallbackActor("job-provider")) : Optional.empty());
        } catch (GeneralSecurityException failure) {
            return CompletableFuture.failedStage(new IllegalStateException("Callback authentication unavailable", failure));
        }
    }
}
