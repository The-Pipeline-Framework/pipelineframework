package org.pipelineframework.connector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Strict, dependency-free reader for static provider artifact metadata.
 */
public final class ConnectorProviderManifestReader {
    private ConnectorProviderManifestReader() {
    }

    public static ConnectorProviderManifest read(InputStream input) {
        Objects.requireNonNull(input, "manifest input must not be null");
        try (input) {
            Object parsed = new JsonParser(new String(input.readAllBytes(), StandardCharsets.UTF_8)).parse();
            return manifest(object(parsed, "manifest"));
        } catch (IOException exception) {
            throw new IllegalArgumentException("unable to read connector provider manifest", exception);
        }
    }

    private static ConnectorProviderManifest manifest(Map<String, Object> root) {
        requireOnly(root, "schemaVersion", "providers");
        List<ConnectorProviderArtifactDescriptor> providers = array(root, "providers").stream()
            .map(value -> artifact(object(value, "provider descriptor")))
            .toList();
        return new ConnectorProviderManifest(integer(root, "schemaVersion"), providers);
    }

    private static ConnectorProviderArtifactDescriptor artifact(Map<String, Object> value) {
        requireOnly(value, "id", "version", "configurationSchema", "executionCapabilities", "operations");
        Optional<ConnectorConfigSchemaDescriptor> schema = optionalSchema(value, "configurationSchema");
        ConnectorProviderDescriptor provider = new ConnectorProviderDescriptor(
            ConnectorProviderId.of(string(value, "id")), version(object(value.get("version"), "provider version")), schema,
            optionalExecutionCapabilities(value));
        if (provider.id().isFrameworkReserved()) {
            throw new IllegalArgumentException(
                "connector provider ID is reserved for framework use: " + provider.id().value());
        }
        List<ConnectorOperationDescriptor> operations = array(value, "operations").stream()
            .map(entry -> operation(object(entry, "operation descriptor")))
            .toList();
        return new ConnectorProviderArtifactDescriptor(provider, operations);
    }

    private static ConnectorOperationDescriptor operation(Map<String, Object> value) {
        requireOnly(value, "id", "kind", "majorVersion", "configurationSchema", "commandCapabilities",
            "queryCapabilities");
        return new ConnectorOperationDescriptor(
            string(value, "id"), ConnectorOperationKind.of(string(value, "kind")), integer(value, "majorVersion"),
            optionalSchema(value, "configurationSchema"), optionalCommandCapabilities(value),
            optionalQueryCapabilities(value));
    }

    private static ConnectorProviderVersion version(Map<String, Object> value) {
        requireOnly(value, "major", "minor");
        return new ConnectorProviderVersion(integer(value, "major"), integer(value, "minor"));
    }

    private static Optional<ConnectorExecutionCapabilities> optionalExecutionCapabilities(Map<String, Object> value) {
        if (!value.containsKey("executionCapabilities")) {
            return Optional.empty();
        }
        Map<String, Object> capabilities = object(value.get("executionCapabilities"), "executionCapabilities");
        requireOnly(capabilities, "executionStyle", "concurrencyScope");
        return Optional.of(new ConnectorExecutionCapabilities(
            enumValue(ConnectorExecutionStyle.class, string(capabilities, "executionStyle"), "executionStyle"),
            enumValue(ConnectorConcurrencyScope.class, string(capabilities, "concurrencyScope"), "concurrencyScope")));
    }

    private static Optional<CommandCapabilities> optionalCommandCapabilities(Map<String, Object> value) {
        if (!value.containsKey("commandCapabilities")) {
            return Optional.empty();
        }
        Map<String, Object> capabilities = object(value.get("commandCapabilities"), "commandCapabilities");
        requireOnly(capabilities,
            "retryRedriveSupported", "providerIdempotencySupported", "reconciliationSupported",
            "executionPosture", "maximumMachineConfirmation", "userConfirmationSupported", "durableReferenceKinds");
        List<String> referenceKinds = array(capabilities, "durableReferenceKinds").stream().map(entry -> {
            if (entry instanceof String string) {
                return string;
            }
            throw malformed("durableReferenceKinds", "string array");
        }).toList();
        return Optional.of(new CommandCapabilities(
            bool(capabilities, "retryRedriveSupported"),
            bool(capabilities, "providerIdempotencySupported"),
            bool(capabilities, "reconciliationSupported"),
            capabilities.containsKey("executionPosture")
                ? enumValue(CommandExecutionPosture.class, string(capabilities, "executionPosture"), "executionPosture")
                : CommandExecutionPosture.UNSPECIFIED,
            enumValue(CommandMachineConfirmation.class, string(capabilities, "maximumMachineConfirmation"),
                "maximumMachineConfirmation"),
            bool(capabilities, "userConfirmationSupported"),
            java.util.Set.copyOf(referenceKinds)));
    }

    private static Optional<QueryCapabilities> optionalQueryCapabilities(Map<String, Object> value) {
        if (!value.containsKey("queryCapabilities")) {
            return Optional.empty();
        }
        Map<String, Object> capabilities = object(value.get("queryCapabilities"), "queryCapabilities");
        requireOnly(capabilities, "cacheability", "maximumCacheAge", "maximumNegativeCacheTtl");
        return Optional.of(new QueryCapabilities(
            enumValue(QueryCacheability.class, string(capabilities, "cacheability"), "cacheability"),
            optionalDuration(capabilities, "maximumCacheAge"),
            optionalDuration(capabilities, "maximumNegativeCacheTtl")));
    }

    private static Optional<java.time.Duration> optionalDuration(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            return Optional.empty();
        }
        try {
            return Optional.of(java.time.Duration.parse(string(value, key)));
        } catch (java.time.format.DateTimeParseException exception) {
            throw malformed(key, "ISO-8601 duration");
        }
    }

    private static Optional<ConnectorConfigSchemaDescriptor> optionalSchema(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            return Optional.empty();
        }
        Map<String, Object> schema = object(value.get(key), key);
        requireOnly(schema, "id", "version", "fields");
        List<ConnectorConfigFieldDescriptor> fields = schema.containsKey("fields")
            ? array(schema, "fields").stream().map(entry -> field(object(entry, "configuration field"))).toList()
            : List.of();
        return Optional.of(new ConnectorConfigSchemaDescriptor(string(schema, "id"), integer(schema, "version"), fields));
    }

    private static ConnectorConfigFieldDescriptor field(Map<String, Object> value) {
        requireOnly(value, "name", "type", "required", "enumValues");
        List<String> enumValues = value.containsKey("enumValues")
            ? array(value, "enumValues").stream().map(entry -> {
                if (entry instanceof String string) {
                    return string;
                }
                throw malformed("enumValues", "string array");
            }).toList()
            : List.of();
        return new ConnectorConfigFieldDescriptor(
            string(value, "name"),
            ConnectorConfigValueType.valueOf(string(value, "type")),
            bool(value, "required"),
            enumValues);
    }

    private static void requireOnly(Map<String, Object> value, String... knownFields) {
        for (String key : value.keySet()) {
            boolean known = false;
            for (String field : knownFields) {
                if (field.equals(key)) {
                    known = true;
                    break;
                }
            }
            if (!known) {
                throw new IllegalArgumentException("malformed connector provider manifest: unsupported field '" + key + "'");
            }
        }
    }

    private static String string(Map<String, Object> value, String key) {
        Object result = required(value, key);
        if (result instanceof String string) {
            return string;
        }
        throw malformed(key, "string");
    }

    private static int integer(Map<String, Object> value, String key) {
        Object result = required(value, key);
        if (result instanceof Integer integer) {
            return integer;
        }
        throw malformed(key, "integer");
    }

    private static boolean bool(Map<String, Object> value, String key) {
        Object result = required(value, key);
        if (result instanceof Boolean bool) {
            return bool;
        }
        throw malformed(key, "boolean");
    }

    private static List<Object> array(Map<String, Object> value, String key) {
        Object result = required(value, key);
        if (result instanceof List<?> list) {
            return List.copyOf(list);
        }
        throw malformed(key, "array");
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> object(Object value, String label) {
        if (value instanceof Map<?, ?> map) {
            return (Map<String, Object>) map;
        }
        throw new IllegalArgumentException("malformed connector provider manifest: " + label + " must be an object");
    }

    private static Object required(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            throw new IllegalArgumentException("malformed connector provider manifest: missing required field '" + key + "'");
        }
        return value.get(key);
    }

    private static IllegalArgumentException malformed(String key, String expected) {
        return new IllegalArgumentException("malformed connector provider manifest: field '" + key + "' must be a " + expected);
    }

    private static <T extends Enum<T>> T enumValue(Class<T> type, String value, String field) {
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException exception) {
            throw malformed(field, type.getSimpleName());
        }
    }

    private static final class JsonParser {
        private final String source;
        private int index;

        private JsonParser(String source) {
            this.source = Objects.requireNonNull(source, "manifest content must not be null");
        }

        private Object parse() {
            Object value = value();
            whitespace();
            if (index != source.length()) {
                throw error("unexpected trailing content");
            }
            return value;
        }

        private Object value() {
            whitespace();
            if (index >= source.length()) {
                throw error("expected a JSON value");
            }
            return switch (source.charAt(index)) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't', 'f' -> bool();
                case 'n' -> unsupportedLiteral();
                default -> number();
            };
        }

        private Map<String, Object> object() {
            expect('{');
            whitespace();
            Map<String, Object> result = new LinkedHashMap<>();
            if (consume('}')) {
                return Map.copyOf(result);
            }
            do {
                whitespace();
                String key = string();
                whitespace();
                expect(':');
                if (result.putIfAbsent(key, value()) != null) {
                    throw error("duplicate field '" + key + "'");
                }
                whitespace();
            } while (consume(','));
            expect('}');
            return Map.copyOf(result);
        }

        private List<Object> array() {
            expect('[');
            whitespace();
            List<Object> result = new ArrayList<>();
            if (consume(']')) {
                return List.copyOf(result);
            }
            do {
                result.add(value());
                whitespace();
            } while (consume(','));
            expect(']');
            return List.copyOf(result);
        }

        private String string() {
            expect('"');
            StringBuilder result = new StringBuilder();
            while (index < source.length()) {
                char current = source.charAt(index++);
                if (current == '"') {
                    return result.toString();
                }
                if (current == '\\') {
                    result.append(escaped());
                } else {
                    result.append(current);
                }
            }
            throw error("unterminated string");
        }

        private char escaped() {
            if (index >= source.length()) {
                throw error("unterminated escape");
            }
            return switch (source.charAt(index++)) {
                case '"' -> '"';
                case '\\' -> '\\';
                case '/' -> '/';
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> unicodeEscape();
                default -> throw error("unsupported string escape");
            };
        }

        private char unicodeEscape() {
            if (index + 4 > source.length()) {
                throw error("malformed unicode escape");
            }
            int value = 0;
            for (int offset = 0; offset < 4; offset++) {
                int digit = Character.digit(source.charAt(index++), 16);
                if (digit < 0) {
                    throw error("malformed unicode escape");
                }
                value = (value << 4) | digit;
            }
            return (char) value;
        }

        private Object unsupportedLiteral() {
            if (source.startsWith("null", index)) {
                index += 4;
                throw error("null values are not supported in connector provider manifests");
            }
            throw error("expected an integer");
        }

        private Integer number() {
            int start = index;
            if (consume('-')) {
                throw error("negative numbers are not supported in connector provider manifests");
            }
            while (index < source.length() && Character.isDigit(source.charAt(index))) {
                index++;
            }
            if (start == index) {
                throw error("expected an integer");
            }
            try {
                return Integer.valueOf(source.substring(start, index));
            } catch (NumberFormatException exception) {
                throw error("integer is out of range");
            }
        }

        private Boolean bool() {
            if (source.startsWith("true", index)) {
                index += 4;
                return Boolean.TRUE;
            }
            if (source.startsWith("false", index)) {
                index += 5;
                return Boolean.FALSE;
            }
            throw error("expected boolean");
        }

        private void whitespace() {
            while (index < source.length() && Character.isWhitespace(source.charAt(index))) {
                index++;
            }
        }

        private boolean consume(char expected) {
            if (index < source.length() && source.charAt(index) == expected) {
                index++;
                return true;
            }
            return false;
        }

        private void expect(char expected) {
            whitespace();
            if (!consume(expected)) {
                throw error("expected '" + expected + "'");
            }
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException("malformed connector provider manifest at offset " + index + ": " + message);
        }
    }
}
