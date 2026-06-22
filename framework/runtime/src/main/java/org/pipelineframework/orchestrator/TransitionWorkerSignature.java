package org.pipelineframework.orchestrator;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

final class TransitionWorkerSignature {

    static final String TIMESTAMP_HEADER = "X-TPF-Worker-Timestamp";
    static final String NONCE_HEADER = "X-TPF-Worker-Nonce";
    static final String SIGNATURE_HEADER = "X-TPF-Worker-Signature";

    private static final HexFormat HEX = HexFormat.of();

    private TransitionWorkerSignature() {
    }

    static String sign(String secret, String method, String path, String timestamp, String nonce, byte[] body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return HEX.formatHex(mac.doFinal(canonicalRequest(method, path, timestamp, nonce, body)
                .getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException("Failed signing transition worker request", e);
        }
    }

    static boolean matches(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        return MessageDigest.isEqual(
            expected.getBytes(StandardCharsets.UTF_8),
            actual.getBytes(StandardCharsets.UTF_8));
    }

    static long parseTimestamp(String timestamp) {
        try {
            return Instant.parse(timestamp).toEpochMilli();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid transition worker signature timestamp", e);
        }
    }

    private static String canonicalRequest(String method, String path, String timestamp, String nonce, byte[] body) {
        return method + "\n"
            + normalizePath(path) + "\n"
            + timestamp + "\n"
            + nonce + "\n"
            + sha256Hex(body);
    }

    private static String normalizePath(String path) {
        if (path == null || path.isBlank()) {
            return "/";
        }
        return path.startsWith("/") ? path : "/" + path;
    }

    private static String sha256Hex(byte[] body) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HEX.formatHex(digest.digest(body == null ? new byte[0] : body));
        } catch (Exception e) {
            throw new IllegalStateException("Failed hashing transition worker request body", e);
        }
    }
}
