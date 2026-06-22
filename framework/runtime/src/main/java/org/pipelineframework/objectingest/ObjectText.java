package org.pipelineframework.objectingest;

import java.util.Optional;

final class ObjectText {

    private ObjectText() {
    }

    static Optional<String> normalize(String value) {
        return value == null || value.isBlank() ? Optional.empty() : Optional.of(value.trim());
    }
}
