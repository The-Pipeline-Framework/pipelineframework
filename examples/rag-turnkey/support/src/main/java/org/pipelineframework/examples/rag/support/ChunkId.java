package org.pipelineframework.examples.rag.support;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;

/** Durable, provider-neutral identity shared by the independent RAG applications. */
public record ChunkId(String sourceId, int index, String contentHash) {
    private static final Pattern ENCODED = Pattern.compile("([A-Za-z0-9_-]+)\\.([0-9]{6})\\.([0-9a-f]{64})");

    public ChunkId {
        sourceId = requireText(sourceId, "source ID");
        if (index < 0 || index > 999_999) throw new IllegalArgumentException("chunk index must be between 0 and 999999");
        contentHash = requireText(contentHash, "chunk content hash");
        if (!contentHash.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("chunk content hash must be lowercase SHA-256");
    }

    public static String encode(String sourceId, int index, String content) {
        String normalizedSource = requireText(sourceId, "source ID");
        if (index < 0 || index > 999_999) {
            throw new IllegalArgumentException("chunk index must be between 0 and 999999");
        }
        String normalizedContent = Objects.requireNonNull(content, "chunk content must not be null");
        String source = Base64.getUrlEncoder().withoutPadding()
            .encodeToString(normalizedSource.getBytes(StandardCharsets.UTF_8));
        return source + "." + "%06d".formatted(index) + "." + sha256(normalizedContent);
    }

    public static ChunkId decode(String encoded) {
        var matcher = ENCODED.matcher(requireText(encoded, "chunk ID"));
        if (!matcher.matches()) throw new IllegalArgumentException("invalid chunk ID");
        try {
            String sourceId = new String(Base64.getUrlDecoder().decode(matcher.group(1)), StandardCharsets.UTF_8);
            ChunkId decoded = new ChunkId(sourceId, Integer.parseInt(matcher.group(2)), matcher.group(3));
            if (!encodePrefix(decoded).equals(encoded)) throw new IllegalArgumentException("chunk ID is not canonical");
            return decoded;
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("invalid chunk ID", failure);
        }
    }

    private static String encodePrefix(ChunkId id) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(id.sourceId().getBytes(StandardCharsets.UTF_8))
            + "." + "%06d".formatted(id.index()) + "." + id.contentHash();
    }

    private static String sha256(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String requireText(String value, String label) {
        String original = Objects.requireNonNull(value, label + " must not be null");
        String normalized = original.trim();
        if (normalized.isEmpty()) throw new IllegalArgumentException(label + " must not be blank");
        if (!original.equals(normalized)) {
            throw new IllegalArgumentException(label + " must not contain surrounding whitespace");
        }
        return original;
    }
}
