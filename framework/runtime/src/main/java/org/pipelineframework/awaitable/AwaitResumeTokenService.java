package org.pipelineframework.awaitable;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import jakarta.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.pipelineframework.config.pipeline.PipelineJson;

/**
 * Issues and validates framework-owned signed resume tokens for external await completions.
 */
@ApplicationScoped
public class AwaitResumeTokenService {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final Base64.Encoder BASE64_URL_ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder BASE64_URL_DECODER = Base64.getUrlDecoder();

    @ConfigProperty(name = "pipeline.orchestrator.resume-token-secret")
    Optional<String> configuredSecret;

    private final String explicitSecret;
    private volatile SecretKeySpec cachedSecretKey;

    public AwaitResumeTokenService() {
        this(null);
    }

    AwaitResumeTokenService(String explicitSecret) {
        this.explicitSecret = explicitSecret;
    }

    /**
     * Signs a token for an await interaction.
     */
    public String sign(AwaitInteractionRecord record, long issuedAtEpochMs) {
        Objects.requireNonNull(record, "record must not be null");
        long issuedAt = issuedAtEpochMs <= 0 ? System.currentTimeMillis() : issuedAtEpochMs;
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("tenantId", record.tenantId());
        payload.put("interactionId", record.interactionId());
        payload.put("correlationId", record.correlationId());
        payload.put("executionId", record.executionId());
        payload.put("stepId", record.stepId());
        payload.put("deadlineEpochMs", record.deadlineEpochMs());
        payload.put("issuedAtEpochMs", issuedAt);
        try {
            byte[] payloadBytes = PipelineJson.mapper().writeValueAsBytes(payload);
            String encodedPayload = BASE64_URL_ENCODER.encodeToString(payloadBytes);
            String encodedSignature = BASE64_URL_ENCODER.encodeToString(signBytes(encodedPayload.getBytes(StandardCharsets.UTF_8)));
            return encodedPayload + "." + encodedSignature;
        } catch (Exception e) {
            throw new IllegalStateException("Failed signing await resume token", e);
        }
    }

    /**
     * Validates a token against the durable interaction record it is intended to complete.
     */
    public void validate(String token, AwaitInteractionRecord record, long nowEpochMs) {
        if (token == null || token.isBlank()) {
            throw new AwaitResumeTokenRejectedException("resumeToken must not be blank");
        }
        Objects.requireNonNull(record, "record must not be null");
        long now = nowEpochMs <= 0 ? System.currentTimeMillis() : nowEpochMs;
        String[] parts = token.split("\\.", -1);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new AwaitResumeTokenRejectedException("Malformed await resume token");
        }
        byte[] expectedSignature = signBytes(parts[0].getBytes(StandardCharsets.UTF_8));
        byte[] suppliedSignature;
        byte[] payloadBytes;
        try {
            suppliedSignature = BASE64_URL_DECODER.decode(parts[1]);
            payloadBytes = BASE64_URL_DECODER.decode(parts[0]);
        } catch (IllegalArgumentException e) {
            throw new AwaitResumeTokenRejectedException("Malformed await resume token", e);
        }
        if (!MessageDigest.isEqual(expectedSignature, suppliedSignature)) {
            throw new AwaitResumeTokenRejectedException("Invalid await resume token signature");
        }
        Map<?, ?> payload;
        try {
            payload = PipelineJson.mapper().readValue(payloadBytes, Map.class);
        } catch (Exception e) {
            throw new AwaitResumeTokenRejectedException("Malformed await resume token payload", e);
        }
        requireEquals(payload, "tenantId", record.tenantId());
        requireEquals(payload, "interactionId", record.interactionId());
        requireEquals(payload, "correlationId", record.correlationId());
        requireEquals(payload, "executionId", record.executionId());
        requireEquals(payload, "stepId", record.stepId());
        long deadline = readLong(payload, "deadlineEpochMs");
        if (deadline != record.deadlineEpochMs()) {
            throw new AwaitResumeTokenRejectedException("Await resume token deadline mismatch");
        }
        if (now > deadline) {
            throw new AwaitResumeTokenRejectedException("Await resume token is expired");
        }
    }

    /**
     * Reads the interaction id from a resume token without trusting it.
     *
     * The value is only a lookup hint; callers must still validate the token
     * against the fetched durable interaction before admitting completion.
     */
    public String interactionIdHint(String token) {
        Map<?, ?> payload = payload(token);
        Object interactionId = payload.get("interactionId");
        if (!(interactionId instanceof String value) || value.isBlank()) {
            throw new AwaitResumeTokenRejectedException("Await resume token does not contain an interaction id");
        }
        return value.trim();
    }

    private Map<?, ?> payload(String token) {
        if (token == null || token.isBlank()) {
            throw new AwaitResumeTokenRejectedException("resumeToken must not be blank");
        }
        String[] parts = token.split("\\.", -1);
        if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new AwaitResumeTokenRejectedException("Malformed await resume token");
        }
        try {
            byte[] payloadBytes = BASE64_URL_DECODER.decode(parts[0]);
            return PipelineJson.mapper().readValue(payloadBytes, Map.class);
        } catch (Exception e) {
            throw new AwaitResumeTokenRejectedException("Malformed await resume token payload", e);
        }
    }

    private byte[] signBytes(byte[] payload) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(secretKey());
            return mac.doFinal(payload);
        } catch (Exception e) {
            throw new IllegalStateException("Failed signing await resume token", e);
        }
    }

    private SecretKeySpec secretKey() {
        SecretKeySpec key = cachedSecretKey;
        if (key == null) {
            synchronized (this) {
                key = cachedSecretKey;
                if (key == null) {
                    key = new SecretKeySpec(secret().getBytes(StandardCharsets.UTF_8), HMAC_ALGORITHM);
                    cachedSecretKey = key;
                }
            }
        }
        return key;
    }

    private String secret() {
        String secret = explicitSecret;
        if (secret == null && configuredSecret != null) {
            secret = configuredSecret.orElse(null);
        }
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException(
                "pipeline.orchestrator.resume-token-secret must be configured for signed await resume tokens");
        }
        return secret;
    }

    private static void requireEquals(Map<?, ?> payload, String key, String expected) {
        Object actual = payload.get(key);
        if (!Objects.equals(expected, actual == null ? null : actual.toString())) {
            throw new AwaitResumeTokenRejectedException("Await resume token " + key + " mismatch");
        }
    }

    private static long readLong(Map<?, ?> payload, String key) {
        Object value = payload.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        if (value instanceof String string && !string.isBlank()) {
            try {
                return Long.parseLong(string);
            } catch (NumberFormatException e) {
                throw new AwaitResumeTokenRejectedException("Await resume token " + key + " is invalid", e);
            }
        }
        throw new AwaitResumeTokenRejectedException("Await resume token " + key + " is missing");
    }
}
