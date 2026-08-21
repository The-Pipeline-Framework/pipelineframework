package org.pipelineframework.file;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.SourceVersion;
import org.pipelineframework.representation.spi.BoundaryRequest;
import org.pipelineframework.representation.spi.ProviderGenerationRequest;

final class FileMappingOptions {
    private static final long DEFAULT_MAX_BYTES = 64L * 1024L * 1024L;
    private static final Set<String> LITERALS = Set.of("true", "false", "null");
    private final ProviderGenerationRequest request;
    private final String role;
    private final Map<String, Object> options;

    private FileMappingOptions(ProviderGenerationRequest request, String role) {
        this.request = request; this.role = role;
        Object value = request.globalConfiguration().get(role);
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' has no " + role + " mapping options.");
        }
        @SuppressWarnings("unchecked") Map<String, Object> typed = (Map<String, Object>) map;
        options = typed;
    }
    static FileMappingOptions input(ProviderGenerationRequest request) { return new FileMappingOptions(request, "input"); }
    static FileMappingOptions output(ProviderGenerationRequest request) { return new FileMappingOptions(request, "output"); }

    boolean structured() { return options.containsKey("fields"); }

    List<String> structuredFields() {
        Map<String, String> fields = fields(); Object configured = options.get("fields");
        if (!(configured instanceof List<?> values) || values.isEmpty()
                || values.stream().anyMatch(v -> !(v instanceof String text) || text.isBlank())) {
            throw new IllegalStateException("Structured file input mapping requires a non-empty string list option 'fields'.");
        }
        List<String> selected = values.stream().map(String.class::cast).map(String::trim).toList();
        selected.forEach(this::validateIdentifier);
        if (selected.stream().distinct().count() != selected.size())
            throw new IllegalStateException("Structured file input mapping fields must be unique.");
        if (fields.size() != selected.size() || selected.stream().anyMatch(f -> !fields.containsKey(f)))
            throw new IllegalStateException("Structured file input boundary '" + request.boundary().stepName()
                + "' requires options.fields to name every field of its mapped record in representation constructor order.");
        if (selected.stream().noneMatch(f -> "payload_ref".equals(fields.get(f))))
            throw new IllegalStateException("Structured file input boundary '" + request.boundary().stepName()
                + "' requires at least one payload_ref field.");
        return selected;
    }

    List<String> materializedFields(List<String> inputFields) {
        Map<String, String> fields = fields();
        List<String> payloads = inputFields.stream().filter(f -> "payload_ref".equals(fields.get(f))).toList();
        Object configured = options.get("materializeFields");
        if (configured == null) return payloads;
        if (!(configured instanceof List<?> values) || values.isEmpty()
                || values.stream().anyMatch(v -> !(v instanceof String text) || text.isBlank()))
            throw new IllegalStateException("Structured file input mapping requires materializeFields to be a non-empty string list.");
        List<String> selected = values.stream().map(String.class::cast).map(String::trim).toList();
        selected.forEach(this::validateIdentifier);
        if (selected.stream().distinct().count() != selected.size() || selected.stream().anyMatch(f -> !payloads.contains(f)))
            throw new IllegalStateException("Structured file input materializeFields must uniquely name payload_ref fields from options.fields.");
        return selected;
    }

    String payloadField() {
        Map<String, String> fields = fields();
        String field = optionalText("field").orElseGet(() -> fields.entrySet().stream()
            .filter(e -> "payload_ref".equals(e.getValue())).map(Map.Entry::getKey).findFirst()
            .orElseThrow(() -> new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' requires a payload_ref field.")));
        validateIdentifier(field);
        if (fields.size() != 1 || !"payload_ref".equals(fields.get(field)))
            throw new IllegalStateException("File boundary '" + request.boundary().stepName()
                + "' requires its mapped record to contain exactly one payload_ref field.");
        return field;
    }

    Map<String, String> fields() { return stringMap(request.boundary().configuration().get(role + "Fields")); }
    long maxBytes() {
        Object value = options.get("maxBytes"); long result;
        if (value == null) result = DEFAULT_MAX_BYTES;
        else if (value instanceof Number number) {
            try {
                result = new BigDecimal(number.toString()).longValueExact();
            } catch (NumberFormatException | ArithmeticException failure) {
                throw invalidMax();
            }
        } else try { result = Long.parseLong(value.toString()); }
        catch (NumberFormatException e) { throw new IllegalStateException("File mapping option 'maxBytes' must be a positive integer.", e); }
        if (result <= 0) throw invalidMax(); return result;
    }
    String requiredText(String name) { return optionalText(name).orElseThrow(() ->
        new IllegalStateException("File output mapping requires option '" + name + "'.")); }
    Optional<String> optionalText(String name) {
        Object value = options.get(name); if (value == null) return Optional.empty();
        if (!(value instanceof String text) || text.isBlank())
            throw new IllegalStateException("File mapping option '" + name + "' must be a non-blank string.");
        return Optional.of(text.trim());
    }
    private IllegalStateException invalidMax() { return new IllegalStateException("File mapping option 'maxBytes' must be a positive integer."); }
    private void validateIdentifier(String field) {
        if (!SourceVersion.isIdentifier(field) || SourceVersion.isKeyword(field) || LITERALS.contains(field))
            throw new IllegalStateException("File boundary '" + request.boundary().stepName() + "' field '"
                + field + "' is not a valid Java identifier.");
    }
    static Set<String> mappingKeys(BoundaryRequest boundary, String name) {
        Object value = boundary.configuration().get(name); if (!(value instanceof List<?> list)) return Set.of();
        return list.stream().filter(String.class::isInstance).map(String.class::cast)
            .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }
    private static Map<String, String> stringMap(Object value) {
        if (!(value instanceof Map<?, ?> map)) return Map.of();
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        map.forEach((k, v) -> { if (k instanceof String name && v instanceof String type) result.put(name, type); });
        return Map.copyOf(result);
    }
}
