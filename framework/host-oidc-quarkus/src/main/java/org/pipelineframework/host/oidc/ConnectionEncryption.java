package org.pipelineframework.host.oidc;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/** AES-GCM envelope; the host supplies and retains versioned encryption keys outside the database. */
public final class ConnectionEncryption {
    private final String currentKey;
    private final Map<String, SecretKey> keys;
    private final SecureRandom random = new SecureRandom();

    public ConnectionEncryption(String currentKey, Map<String, SecretKey> keys) {
        this.currentKey = Objects.requireNonNull(currentKey);
        this.keys = Map.copyOf(keys);
        if (!currentKey.matches("[a-zA-Z0-9_-]{1,64}") || !keys.containsKey(currentKey)) {
            throw new IllegalArgumentException("Invalid encryption key configuration");
        }
    }

    String encrypt(String authority, byte[] plaintext) {
        byte[] nonce = new byte[12];
        random.nextBytes(nonce);
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, keys.get(currentKey), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(authority.getBytes(StandardCharsets.UTF_8));
            return currentKey + "." + encode(nonce) + "." + encode(cipher.doFinal(plaintext));
        } catch (GeneralSecurityException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }

    byte[] decrypt(String authority, String envelope) {
        try {
            String[] parts = envelope.split("\\.", -1);
            if (parts.length != 3 || !keys.containsKey(parts[0])) {
                throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
            }
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, keys.get(parts[0]),
                new GCMParameterSpec(128, Base64.getUrlDecoder().decode(parts[1])));
            cipher.updateAAD(authority.getBytes(StandardCharsets.UTF_8));
            return cipher.doFinal(Base64.getUrlDecoder().decode(parts[2]));
        } catch (GeneralSecurityException | IllegalArgumentException failure) {
            throw new ConnectionFailure(ConnectionFailure.Reason.STORAGE);
        }
    }

    String nonce() {
        byte[] bytes = new byte[32];
        random.nextBytes(bytes);
        return encode(bytes);
    }

    String hash(String value) {
        try {
            return encode(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (GeneralSecurityException failure) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    private String encode(byte[] bytes) { return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
}
