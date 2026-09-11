package org.pipelineframework.connector;

import java.util.Objects;

/** Application-selected audit identity; never a credential or raw request value. */
public record ProviderCallbackActor(String value) {
    public ProviderCallbackActor {
        Objects.requireNonNull(value, "value");
        if (!value.matches("[A-Za-z0-9._:-]{1,128}")) {
            throw new IllegalArgumentException("callback actor must be a bounded audit identifier");
        }
    }
}
