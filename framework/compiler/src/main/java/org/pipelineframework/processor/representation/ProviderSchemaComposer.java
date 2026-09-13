package org.pipelineframework.processor.representation;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.pipelineframework.representation.spi.ProviderSchemaFragment;

/** Deterministic composition point; provider fragments remain opaque JSON to core. */
public final class ProviderSchemaComposer {
    public String compose(List<ProviderSchemaFragment> fragments) {
        String entries = (fragments == null ? List.<ProviderSchemaFragment>of() : fragments).stream()
            .sorted(Comparator.comparing(ProviderSchemaFragment::providerKey))
            .map(fragment -> "\"" + fragment.providerKey() + "\":{"
                + "\"global\":" + fragment.globalSchemaJson().orElse("{}") + ","
                + "\"type\":" + fragment.typeSchemaJson().orElse("{}") + "}")
            .collect(Collectors.joining(","));
        return "{\"representationProviders\":{" + entries + "}}";
    }
}
