package org.pipelineframework.representation.spi;

import java.util.Optional;

/** Provider-owned schema and documentation fragments composed deterministically by the host. */
public record ProviderSchemaFragment(String providerKey, Optional<String> globalSchemaJson,
                                     Optional<String> typeSchemaJson, Optional<String> documentationMarkdown) {
    public ProviderSchemaFragment {
        if (providerKey == null || providerKey.isBlank()) {
            throw new IllegalArgumentException("providerKey must not be blank");
        }
        providerKey = providerKey.trim();
        globalSchemaJson = globalSchemaJson == null ? Optional.empty() : globalSchemaJson;
        typeSchemaJson = typeSchemaJson == null ? Optional.empty() : typeSchemaJson;
        documentationMarkdown = documentationMarkdown == null ? Optional.empty() : documentationMarkdown;
    }
}
