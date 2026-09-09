package org.pipelineframework.connector.http;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.fasterxml.jackson.databind.JsonNode;

/** Normalized bounded wire schema retained without its source OpenAPI document. */
public final class HttpWireSchema {
    private final String canonicalJson;
    private final String sha256;
    private final JsonNode parsed;
    private final Map<String, Pattern> patterns;

    public HttpWireSchema(String canonicalJson) {
        this(normalize(canonicalJson), Optional.empty());
    }

    public HttpWireSchema(String canonicalJson, String sha256) {
        this(normalize(canonicalJson), Optional.of(requireText(sha256, "wire schema fingerprint")));
    }

    private HttpWireSchema(Normalized normalized, Optional<String> suppliedFingerprint) {
        this.canonicalJson = normalized.canonicalJson();
        this.sha256 = HttpPinnedJson.sha256(canonicalJson);
        suppliedFingerprint.ifPresent(value -> {
            if (!this.sha256.equals(value)) {
                throw new IllegalArgumentException("wire schema fingerprint mismatch");
            }
        });
        this.parsed = normalized.parsed();
        this.patterns = compilePatterns(parsed);
    }

    public String canonicalJson() {
        return canonicalJson;
    }

    public String sha256() {
        return sha256;
    }

    /** Returns an isolated tree so callers cannot mutate the release-pinned schema. */
    public JsonNode node() {
        return parsed.deepCopy();
    }

    boolean matchesPattern(String expression, String value) {
        Pattern pattern = patterns.get(expression);
        if (pattern == null) {
            throw new IllegalStateException("pinned HTTP schema pattern was not compiled: " + expression);
        }
        return pattern.matcher(value).find();
    }

    private static Normalized normalize(String value) {
        JsonNode parsed = HttpPinnedJson.parse(requireText(value, "wire schema"));
        if (!parsed.isObject() && !parsed.isBoolean()) {
            throw new IllegalArgumentException("wire schema must be a JSON Schema object or boolean");
        }
        return new Normalized(HttpPinnedJson.canonicalize(parsed), parsed.deepCopy());
    }

    private static Map<String, Pattern> compilePatterns(JsonNode schema) {
        Map<String, Pattern> result = new LinkedHashMap<>();
        collectPatterns(schema, result);
        return Map.copyOf(result);
    }

    private static void collectPatterns(JsonNode node, Map<String, Pattern> target) {
        if (node.isObject()) {
            JsonNode pattern = node.path("pattern");
            if (pattern.isTextual()) {
                target.computeIfAbsent(pattern.textValue(), HttpWireSchema::compilePattern);
            }
            node.elements().forEachRemaining(child -> collectPatterns(child, target));
        } else if (node.isArray()) {
            node.elements().forEachRemaining(child -> collectPatterns(child, target));
        }
    }

    private static Pattern compilePattern(String expression) {
        try {
            return Pattern.compile(expression);
        } catch (PatternSyntaxException invalidPin) {
            throw new IllegalArgumentException("pinned HTTP schema contains an invalid pattern", invalidPin);
        }
    }

    private static String requireText(String value, String subject) {
        String result = Objects.requireNonNull(value, subject + " must not be null").trim();
        if (result.isEmpty()) throw new IllegalArgumentException(subject + " must not be blank");
        return result;
    }

    @Override
    public boolean equals(Object candidate) {
        return candidate instanceof HttpWireSchema other
            && canonicalJson.equals(other.canonicalJson) && sha256.equals(other.sha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canonicalJson, sha256);
    }

    @Override
    public String toString() {
        return "HttpWireSchema[canonicalJson=" + canonicalJson + ", sha256=" + sha256 + "]";
    }

    private record Normalized(String canonicalJson, JsonNode parsed) {
    }
}
