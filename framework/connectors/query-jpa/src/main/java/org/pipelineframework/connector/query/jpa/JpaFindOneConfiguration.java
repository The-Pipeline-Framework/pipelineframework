package org.pipelineframework.connector.query.jpa;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Bindable operation configuration for the {@code jpa.query/find.one} Query operation. */
public record JpaFindOneConfiguration(
    String entity,
    Map<String, JpaPredicate> where,
    Optional<Map<String, String>> projection,
    Optional<Map<String, String>> orderBy,
    Optional<Integer> limit,
    Optional<String> result
) {
    public JpaFindOneConfiguration {
        entity = Objects.requireNonNull(entity, "JPA entity must not be null");
        where = immutable(where, "JPA predicates must not be null");
        projection = Objects.requireNonNull(projection, "JPA projection must not be null")
            .map(values -> immutable(values, "JPA projection values must not be null"));
        orderBy = Objects.requireNonNull(orderBy, "JPA ordering must not be null")
            .map(values -> immutable(values, "JPA ordering values must not be null"));
        limit = Objects.requireNonNull(limit, "JPA limit must not be null");
        result = Objects.requireNonNull(result, "JPA result mode must not be null");
    }

    private static <K, V> Map<K, V> immutable(Map<K, V> values, String message) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, message)));
    }
}
