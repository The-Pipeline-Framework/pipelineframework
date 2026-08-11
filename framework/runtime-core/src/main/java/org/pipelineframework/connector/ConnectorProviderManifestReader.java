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
        requireOnly(value, "id", "version", "configurationSchema", "operations");
        Optional<ConnectorConfigSchemaDescriptor> schema = optionalSchema(value, "configurationSchema");
        ConnectorProviderDescriptor provider = new ConnectorProviderDescriptor(
            ConnectorProviderId.of(string(value, "id")), version(object(value.get("version"), "provider version")), schema);
        List<ConnectorOperationDescriptor> operations = array(value, "operations").stream()
            .map(entry -> operation(object(entry, "operation descriptor")))
            .toList();
        return new ConnectorProviderArtifactDescriptor(provider, operations);
    }

    private static ConnectorOperationDescriptor operation(Map<String, Object> value) {
        requireOnly(value, "id", "kind", "majorVersion", "configurationSchema");
        return new ConnectorOperationDescriptor(
            string(value, "id"), ConnectorOperationKind.of(string(value, "kind")), integer(value, "majorVersion"),
            optionalSchema(value, "configurationSchema"));
    }

    private static ConnectorProviderVersion version(Map<String, Object> value) {
        requireOnly(value, "major", "minor");
        return new ConnectorProviderVersion(integer(value, "major"), integer(value, "minor"));
    }

    private static Optional<ConnectorConfigSchemaDescriptor> optionalSchema(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            return Optional.empty();
        }
        Map<String, Object> schema = object(value.get(key), key);
        requireOnly(schema, "id", "version");
        return Optional.of(new ConnectorConfigSchemaDescriptor(string(schema, "id"), integer(schema, "version")));
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
                default -> throw error("unsupported string escape");
            };
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
