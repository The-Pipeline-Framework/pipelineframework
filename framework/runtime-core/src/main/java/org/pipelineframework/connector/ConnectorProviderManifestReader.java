package org.pipelineframework.connector;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.pipelineframework.config.template.PipelineTemplateScalarTypes;
import org.pipelineframework.config.template.ProtocolTypeReferences;
import org.pipelineframework.config.template.PipelineTemplateTypeDefinition;
import org.pipelineframework.config.template.PipelineTemplateTypeReference;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraintValidator;
import org.pipelineframework.config.template.PipelineTemplateWrapperConstraints;
import org.pipelineframework.protocol.ProtocolTypeDescriptor;
import org.pipelineframework.protocol.ProtocolTypeIdentity;

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
        int schemaVersion = integer(root, "schemaVersion");
        List<ConnectorProviderArtifactDescriptor> providers = array(root, "providers").stream()
            .map(value -> artifact(object(value, "provider descriptor"), schemaVersion))
            .toList();
        return new ConnectorProviderManifest(schemaVersion, providers);
    }

    private static ConnectorProviderArtifactDescriptor artifact(Map<String, Object> value, int schemaVersion) {
        requireOnly(value, "id", "version", "configurationSchema", "operations", "protocolTypes");
        Optional<ConnectorConfigSchemaDescriptor> schema = optionalSchema(value, "configurationSchema");
        ConnectorProviderDescriptor provider = new ConnectorProviderDescriptor(
            ConnectorProviderId.of(string(value, "id")), version(object(value.get("version"), "provider version")), schema);
        if (provider.id().isFrameworkReserved()) {
            throw new IllegalArgumentException(
                "connector provider ID is reserved for framework use: " + provider.id().value());
        }
        List<ConnectorOperationDescriptor> operations = array(value, "operations").stream()
            .map(entry -> operation(object(entry, "operation descriptor"), schemaVersion))
            .toList();
        if (schemaVersion == 1 && value.containsKey("protocolTypes")) {
            throw new IllegalArgumentException("connector provider manifest schema version 1 cannot declare protocolTypes");
        }
        List<ProtocolTypeDescriptor> protocolTypes = value.containsKey("protocolTypes")
            ? array(value, "protocolTypes").stream()
                .map(entry -> protocolType(provider.id(), object(entry, "protocol type descriptor"))).toList()
            : List.of();
        return new ConnectorProviderArtifactDescriptor(provider, operations, protocolTypes);
    }

    private static ProtocolTypeDescriptor protocolType(ConnectorProviderId namespace, Map<String, Object> value) {
        requireOnly(value, "name", "fields", "wraps", "alias", "variants",
            "minLength", "maxLength", "pattern", "format", "minimum", "minimumExclusive", "maximum", "maximumExclusive");
        String name = string(value, "name");
        boolean fields = value.containsKey("fields");
        boolean wraps = value.containsKey("wraps");
        boolean alias = value.containsKey("alias");
        boolean variants = value.containsKey("variants");
        if ((fields ? 1 : 0) + (wraps ? 1 : 0) + (alias ? 1 : 0) + (variants ? 1 : 0) != 1) {
            throw new IllegalArgumentException("protocol type '" + name
                + "' must declare exactly one of fields, wraps, alias, or variants");
        }
        PipelineTemplateTypeDefinition definition;
        if (fields) {
            rejectConstraints(value, name);
            List<PipelineTemplateTypeDefinition.Field> parsed = array(value, "fields").stream()
                .map(entry -> protocolField(name, object(entry, "protocol type field"))).toList();
            if (parsed.stream().map(PipelineTemplateTypeDefinition.Field::name).distinct().count() != parsed.size()) {
                throw new IllegalArgumentException("protocol type '" + name + "' declares duplicate field names");
            }
            definition = new PipelineTemplateTypeDefinition.RecordType(name, parsed);
        } else if (wraps) {
            String scalar = string(value, "wraps");
            if (!PipelineTemplateScalarTypes.isScalar(scalar)) {
                throw new IllegalArgumentException("protocol type '" + name + "' wraps must reference a supported scalar");
            }
            PipelineTemplateWrapperConstraints constraints = protocolConstraints(value, name, scalar);
            definition = new PipelineTemplateTypeDefinition.WrapperType(
                name, new PipelineTemplateTypeReference.Scalar(scalar), constraints);
        } else if (alias) {
            rejectConstraints(value, name);
            definition = new PipelineTemplateTypeDefinition.AliasType(name, protocolReference(string(value, "alias"), name));
        } else {
            rejectConstraints(value, name);
            Map<String, Object> declared = object(value.get("variants"), "protocol type variants");
            if (declared.isEmpty()) {
                throw new IllegalArgumentException("protocol union '" + name + "' must declare at least one variant");
            }
            Map<String, PipelineTemplateTypeDefinition.Variant> parsed = new LinkedHashMap<>();
            declared.forEach((discriminator, payload) -> {
                if (!(payload instanceof String type)) {
                    throw malformed("variants." + discriminator, "string");
                }
                PipelineTemplateTypeReference reference = protocolReference(type, name + "." + discriminator);
                if (!(reference instanceof PipelineTemplateTypeReference.Contributed)) {
                    throw new IllegalArgumentException("protocol union '" + name + "' variant '" + discriminator
                        + "' must reference a contributed protocol type");
                }
                parsed.put(discriminator, new PipelineTemplateTypeDefinition.Variant(discriminator, reference));
            });
            definition = new PipelineTemplateTypeDefinition.UnionType(name, parsed);
        }
        return new ProtocolTypeDescriptor(new ProtocolTypeIdentity(namespace, name), definition);
    }

    private static PipelineTemplateTypeDefinition.Field protocolField(String owner, Map<String, Object> value) {
        requireOnly(value, "name", "type");
        String name = string(value, "name");
        return new PipelineTemplateTypeDefinition.Field(name, protocolReference(string(value, "type"), owner + "." + name));
    }

    private static PipelineTemplateTypeReference protocolReference(String value, String owner) {
        String token = value.trim();
        if (PipelineTemplateScalarTypes.isScalar(token)) {
            return new PipelineTemplateTypeReference.Scalar(token);
        }
        try {
            Optional<String> identity = ProtocolTypeReferences.parseContributed(token);
            if (identity.isPresent()) {
                ProtocolTypeIdentity.of(identity.orElseThrow());
                return new PipelineTemplateTypeReference.Contributed(identity.orElseThrow());
            }
        } catch (IllegalArgumentException failure) {
            throw new IllegalArgumentException("protocol type '" + owner
                + "' must reference a supported scalar or qualified contributed type, got '" + value + "'", failure);
        }
        throw new IllegalArgumentException("protocol type '" + owner
            + "' must reference a supported scalar or qualified contributed type, got '" + value + "'");
    }

    private static PipelineTemplateWrapperConstraints protocolConstraints(
        Map<String, Object> value,
        String name,
        String scalar
    ) {
        Optional<Integer> minLength = optionalInteger(value, "minLength");
        Optional<Integer> maxLength = optionalInteger(value, "maxLength");
        Optional<String> pattern = optionalString(value, "pattern");
        Optional<PipelineTemplateWrapperConstraints.Format> format = optionalString(value, "format")
            .map(entry -> {
                if (!"email".equals(entry)) {
                    throw new IllegalArgumentException("protocol type '" + name + "' format must be 'email'");
                }
                return PipelineTemplateWrapperConstraints.Format.EMAIL;
            });
        Optional<BigDecimal> minimum = optionalDecimal(value, "minimum");
        Optional<BigDecimal> minimumExclusive = optionalDecimal(value, "minimumExclusive");
        Optional<BigDecimal> maximum = optionalDecimal(value, "maximum");
        Optional<BigDecimal> maximumExclusive = optionalDecimal(value, "maximumExclusive");
        pattern.ifPresent(expression -> {
            try {
                Pattern.compile(expression);
            } catch (PatternSyntaxException exception) {
                throw new IllegalArgumentException("protocol type '" + name + "' pattern is not supported: "
                    + exception.getDescription());
            }
        });
        PipelineTemplateWrapperConstraints constraints = new PipelineTemplateWrapperConstraints(
            minLength, maxLength, pattern, format, minimum, minimumExclusive, maximum, maximumExclusive);
        PipelineTemplateWrapperConstraintValidator.findViolation(scalar, constraints)
            .ifPresent(violation -> { throw protocolConstraintFailure(name, violation); });
        return constraints;
    }

    private static IllegalArgumentException protocolConstraintFailure(
        String name,
        PipelineTemplateWrapperConstraintValidator.Violation violation
    ) {
        String message = switch (violation.kind()) {
            case STRING_ON_NON_STRING -> "uses string constraints on non-string wrapper";
            case NUMERIC_ON_NON_NUMERIC -> "uses numeric constraints on non-numeric wrapper";
            case PATTERN_REQUIRES_MAX_LENGTH -> "pattern requires maxLength";
            case PATTERN_TOO_LONG -> "pattern exceeds the supported maximum length of "
                + PipelineTemplateWrapperConstraintValidator.MAX_PATTERN_LENGTH;
            case PATTERN_INPUT_TOO_LONG -> "pattern maxLength exceeds the runtime matching limit of "
                + PipelineTemplateWrapperConstraintValidator.MAX_PATTERN_INPUT_LENGTH;
            case UNSAFE_PATTERN -> "pattern uses a regex feature that is unsafe for runtime model validation";
            case MIN_LENGTH_EXCEEDS_MAX_LENGTH -> "minLength must not exceed maxLength";
            case LOWER_BOUNDS_COMBINED, UPPER_BOUNDS_COMBINED ->
                "cannot declare inclusive and exclusive bounds together";
            case EMPTY_INTERVAL -> "declares an empty numeric constraint interval";
        };
        return new IllegalArgumentException("protocol type '" + name + "' " + message);
    }

    private static void rejectConstraints(Map<String, Object> value, String name) {
        for (String key : List.of("minLength", "maxLength", "pattern", "format", "minimum", "minimumExclusive",
            "maximum", "maximumExclusive")) {
            if (value.containsKey(key)) {
                throw new IllegalArgumentException("protocol type '" + name + "' can declare '" + key + "' only beside wraps");
            }
        }
    }

    private static Optional<Integer> optionalInteger(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            return Optional.empty();
        }
        int result = integer(value, key);
        if (result < 0) {
            throw new IllegalArgumentException("protocol type constraint '" + key + "' must be non-negative");
        }
        return Optional.of(result);
    }

    private static Optional<String> optionalString(Map<String, Object> value, String key) {
        return value.containsKey(key) ? Optional.of(string(value, key)) : Optional.empty();
    }

    private static Optional<BigDecimal> optionalDecimal(Map<String, Object> value, String key) {
        if (!value.containsKey(key)) {
            return Optional.empty();
        }
        Object raw = value.get(key);
        if (!(raw instanceof Number)) {
            throw malformed(key, "number");
        }
        return Optional.of(new BigDecimal(raw.toString()).stripTrailingZeros());
    }

    private static ConnectorOperationDescriptor operation(Map<String, Object> value, int schemaVersion) {
        requireOnly(value, "id", "kind", "majorVersion", "configurationSchema", "commandCapabilities",
            "queryCapabilities", "queryCardinality", "typeContract");
        if (schemaVersion == 1 && value.containsKey("typeContract")) {
            throw malformed("typeContract", "field absent from schema version 1");
        }
        ConnectorOperationKind kind = ConnectorOperationKind.of(string(value, "kind"));
        Optional<QueryOperationCardinality> queryCardinality = queryCardinality(value, schemaVersion, kind);
        return new ConnectorOperationDescriptor(
            string(value, "id"), kind, integer(value, "majorVersion"),
            optionalSchema(value, "configurationSchema"), optionalCommandCapabilities(value),
            optionalQueryCapabilities(value), queryCardinality, optionalTypeContract(value));
    }

    private static Optional<QueryOperationCardinality> queryCardinality(
        Map<String, Object> value,
        int schemaVersion,
        ConnectorOperationKind kind
    ) {
        if (!ConnectorOperationKind.QUERY.equals(kind)) {
            if (value.containsKey("queryCardinality")) {
                throw malformed("queryCardinality", "field valid only for Query operations");
            }
            return Optional.empty();
        }
        if (!value.containsKey("queryCardinality")) {
            if (schemaVersion >= 4) {
                throw malformed("queryCardinality", "field required for Query operations in schema version 4");
            }
            return Optional.of(QueryOperationCardinality.ONE_TO_ONE);
        }
        return Optional.of(QueryOperationCardinality.of(string(value, "queryCardinality")));
    }

    private static Optional<ConnectorOperationTypeContract> optionalTypeContract(Map<String, Object> value) {
        if (!value.containsKey("typeContract")) {
            return Optional.empty();
        }
        Map<String, Object> contract = object(value.get("typeContract"), "typeContract");
        requireOnly(contract, "input", "output");
        return Optional.of(new ConnectorOperationTypeContract(
            string(contract, "input"),
            contract.containsKey("output") ? Optional.of(string(contract, "output")) : Optional.empty()));
    }

    private static ConnectorProviderVersion version(Map<String, Object> value) {
        requireOnly(value, "major", "minor");
        return new ConnectorProviderVersion(integer(value, "major"), integer(value, "minor"));
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
