package org.pipelineframework.connector.query.jpa;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Bindable finite-streaming configuration shared by Hibernate Query providers. */
public record JpaFindManyConfiguration(
    String entity,
    Map<String, JpaPredicate> where,
    Optional<Map<String, String>> projection,
    Map<String, String> orderBy,
    List<String> uniqueBy,
    Optional<Integer> limit
) {
    public JpaFindManyConfiguration {
        entity = Objects.requireNonNull(entity, "JPA entity must not be null");
        where = immutable(where, "JPA predicates must not be null");
        projection = Objects.requireNonNull(projection, "JPA projection must not be null")
            .map(values -> immutable(values, "JPA projection values must not be null"));
        orderBy = immutable(orderBy, "JPA ordering must not be null");
        uniqueBy = List.copyOf(Objects.requireNonNull(uniqueBy, "JPA unique ordering must not be null"));
        limit = Objects.requireNonNull(limit, "JPA limit must not be null");
    }

    private static <K, V> Map<K, V> immutable(Map<K, V> values, String message) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(values, message)));
    }
}
