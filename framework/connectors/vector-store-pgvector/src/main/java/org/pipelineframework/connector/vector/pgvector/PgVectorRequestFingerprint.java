package org.pipelineframework.connector.vector.pgvector;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import org.pipelineframework.connector.vector.VectorUpsertRequest;

/** Stable provider-side fingerprint used to detect command-ID conflicts. */
final class PgVectorRequestFingerprint {
    private PgVectorRequestFingerprint() {
    }

    static String of(VectorUpsertRequest request) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            update(digest, request.itemId());
            update(digest, request.content());
            ByteBuffer bits = ByteBuffer.allocate(Integer.BYTES);
            for (Float value : request.values()) {
                bits.clear();
                bits.putInt(Float.floatToRawIntBits(value));
                digest.update(bits.array());
            }
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static void update(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
        digest.update(bytes);
    }
}
