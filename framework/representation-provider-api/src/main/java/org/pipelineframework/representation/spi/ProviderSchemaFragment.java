package org.pipelineframework.representation.spi;

import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

/** Provider-owned schema and documentation fragments composed deterministically by the host. */
public record ProviderSchemaFragment(String providerKey, Optional<String> globalSchemaJson,
                                     Optional<String> typeSchemaJson, Optional<String> documentationMarkdown) {
    private static final JsonFactory JSON = JsonFactory.builder().build();

    public ProviderSchemaFragment {
        if (providerKey == null || !providerKey.trim().matches("[A-Za-z0-9][A-Za-z0-9._-]*")) {
            throw new IllegalArgumentException("providerKey must be a JSON-safe schema key");
        }
        providerKey = providerKey.trim();
        globalSchemaJson = normalizedJson(globalSchemaJson, "globalSchemaJson");
        typeSchemaJson = normalizedJson(typeSchemaJson, "typeSchemaJson");
        documentationMarkdown = documentationMarkdown == null ? Optional.empty() : documentationMarkdown;
    }

    private static Optional<String> normalizedJson(Optional<String> value, String fieldName) {
        if (value == null || value.isEmpty()) {
            return Optional.empty();
        }
        String json = value.orElseThrow().trim();
        if (json.isEmpty()) {
            throw new IllegalArgumentException(fieldName + " must not be blank when present");
        }
        try (JsonParser parser = JSON.createParser(json)) {
            if (parser.nextToken() == null) {
                throw new IllegalArgumentException(fieldName + " must contain a JSON value");
            }
            parser.skipChildren();
            if (parser.nextToken() != null) {
                throw new IllegalArgumentException(fieldName + " must contain exactly one JSON value");
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(fieldName + " must contain valid JSON", e);
        }
        return Optional.of(json);
    }
}
