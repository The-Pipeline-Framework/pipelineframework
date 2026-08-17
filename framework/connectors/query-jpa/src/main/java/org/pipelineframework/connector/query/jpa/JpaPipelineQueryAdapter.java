package org.pipelineframework.connector.query.jpa;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.pipelineframework.config.pipeline.PipelineYamlJpaPredicate;
import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;

/** Converts pipeline transport configuration into the connector's provider-neutral domain. */
final class JpaPipelineQueryAdapter {
    private JpaPipelineQueryAdapter() {
    }

    static JpaFindOneConfiguration configuration(PipelineYamlJpaQuery query) {
        Map<String, JpaPredicate> where = new LinkedHashMap<>();
        query.where().forEach((path, predicate) -> where.put(path, predicate(predicate)));
        return new JpaFindOneConfiguration(
            query.entity(),
            where,
            optionalMap(query.projection()),
            optionalMap(query.orderBy()),
            Optional.ofNullable(query.limit()),
            Optional.of(query.result()));
    }

    private static JpaPredicate predicate(PipelineYamlJpaPredicate predicate) {
        return new JpaPredicate(predicate.operator(), predicate.values());
    }

    private static <K, V> Optional<Map<K, V>> optionalMap(Map<K, V> values) {
        return values.isEmpty() ? Optional.empty() : Optional.of(values);
    }
}
