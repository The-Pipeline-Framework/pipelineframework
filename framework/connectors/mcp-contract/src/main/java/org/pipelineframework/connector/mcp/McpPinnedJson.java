package org.pipelineframework.connector.mcp;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/** Deterministic JSON for private MCP pins. Does not resolve references or perform I/O. */
public final class McpPinnedJson {
    public static final int MAX_BYTES = 1_048_576;
    public static final int MAX_DEPTH = 64;
    private static final int MAX_NODES = 20_000;
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .streamReadConstraints(StreamReadConstraints.builder().maxNestingDepth(MAX_DEPTH)
            .maxStringLength(MAX_BYTES).maxNumberLength(4096).build()).build())
        .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
        .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
        .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private McpPinnedJson() { }

    public static JsonNode parse(String json) {
        if (json.length() > MAX_BYTES || json.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new IllegalArgumentException("MCP JSON exceeds size limit");
        }
        try {
            JsonNode value = JSON.readTree(json);
            if (value == null) {
                throw new IllegalArgumentException("MCP JSON must not be empty");
            }
            return canonical(value, 0, new int[] {0});
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("invalid MCP JSON", failure);
        }
    }

    public static String canonicalize(JsonNode value) {
        String json = canonical(value, 0, new int[] {0}).toString();
        if (json.length() > MAX_BYTES || json.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new IllegalArgumentException("MCP JSON exceeds size limit");
        }
        return json;
    }

    public static String sha256(String canonicalJson) {
        try {
            return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(canonicalJson.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 unavailable", failure);
        }
    }

    private static JsonNode canonical(JsonNode value, int depth, int[] nodes) {
        if (depth > MAX_DEPTH || ++nodes[0] > MAX_NODES) {
            throw new IllegalArgumentException("MCP JSON exceeds depth or node limit");
        }
        if (value.isObject()) {
            var result = JsonNodeFactory.instance.objectNode();
            var keys = new ArrayList<String>();
            value.fieldNames().forEachRemaining(keys::add);
            keys.stream().sorted().forEach(key -> result.set(key, canonical(value.get(key), depth + 1, nodes)));
            return result;
        }
        if (value.isArray()) {
            var result = JsonNodeFactory.instance.arrayNode();
            value.forEach(item -> result.add(canonical(item, depth + 1, nodes)));
            return result;
        }
        if (value.isNumber()) {
            if ((value.isDouble() || value.isFloat()) && !Double.isFinite(value.doubleValue())) {
                throw new IllegalArgumentException("MCP JSON numbers must be finite");
            }
            var number = value.decimalValue().stripTrailingZeros();
            if (number.precision() > 2048 || Math.abs((long) number.scale()) > 1024
                || (long) number.precision() - number.scale() > 2048) {
                throw new IllegalArgumentException("MCP JSON number exceeds precision or exponent limit");
            }
            return number.scale() <= 0 ? JsonNodeFactory.instance.numberNode(number.toBigIntegerExact())
                : JsonNodeFactory.instance.numberNode(number);
        }
        if (value.isTextual() || value.isBoolean() || value.isNull()) {
            return value;
        }
        throw new IllegalArgumentException("MCP pin contains a non-JSON value");
    }
}
