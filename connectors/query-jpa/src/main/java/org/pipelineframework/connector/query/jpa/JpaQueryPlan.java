package org.pipelineframework.connector.query.jpa;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.config.pipeline.PipelineYamlJpaPredicate;
import org.pipelineframework.query.QueryStepDescriptor;

final class JpaQueryPlan {
    private static final Pattern JAVA_IDENTIFIER = Pattern.compile("[A-Za-z_$][A-Za-z\\d_$]*");
    private static final String INPUT_PREFIX = "input.";
    private static final Set<String> ORDER_DIRECTIONS = Set.of("asc", "desc");

    private final String queryId;
    private final Class<?> entityType;
    private final Map<String, PipelineYamlJpaPredicate> where;
    private final Map<String, String> projection;
    private final Map<String, String> orderBy;
    private final Integer limit;

    private JpaQueryPlan(
        String queryId,
        Class<?> entityType,
        Map<String, PipelineYamlJpaPredicate> where,
        Map<String, String> projection,
        Map<String, String> orderBy,
        Integer limit
    ) {
        this.queryId = queryId;
        this.entityType = entityType;
        this.where = Collections.unmodifiableMap(new LinkedHashMap<>(where));
        this.projection = Collections.unmodifiableMap(new LinkedHashMap<>(projection));
        this.orderBy = Collections.unmodifiableMap(new LinkedHashMap<>(orderBy));
        this.limit = limit;
    }

    static JpaQueryPlan from(QueryStepDescriptor descriptor) {
        PipelineYamlJpaQuery jpa = descriptor.jpa();
        Class<?> entityType = loadClass(jpa.entity());
        validatePathMap(jpa.where(), "where");
        validatePropertyMap(jpa.projection(), "projection");
        validateOrderByMap(jpa.orderBy());
        return new JpaQueryPlan(descriptor.queryId(), entityType, jpa.where(), jpa.projection(), jpa.orderBy(), jpa.limit());
    }

    String queryId() {
        return queryId;
    }

    Class<?> entityType() {
        return entityType;
    }

    Map<String, String> projection() {
        return projection;
    }

    String toHql() {
        StringBuilder hql = new StringBuilder("select e from ")
            .append(entityType.getName())
            .append(" e where ");
        int index = 0;
        ParameterCounter parameters = new ParameterCounter();
        for (Map.Entry<String, PipelineYamlJpaPredicate> entry : where.entrySet()) {
            if (index > 0) {
                hql.append(" and ");
            }
            appendPredicate(hql, entry.getKey(), entry.getValue(), parameters);
            index++;
        }
        if (!orderBy.isEmpty()) {
            hql.append(" order by ");
            int orderIndex = 0;
            for (Map.Entry<String, String> entry : orderBy.entrySet()) {
                if (orderIndex > 0) {
                    hql.append(", ");
                }
                hql.append("e.").append(entry.getKey()).append(" ").append(entry.getValue());
                orderIndex++;
            }
        }
        return hql.toString();
    }

    Map<String, Object> bindings(Object input) {
        Map<String, Object> bindings = new LinkedHashMap<>();
        ParameterCounter parameters = new ParameterCounter();
        for (PipelineYamlJpaPredicate predicate : where.values()) {
            bindPredicate(bindings, predicate, input, parameters);
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(bindings));
    }

    int maxResults() {
        return limit != null && limit == 1 ? 1 : 2;
    }

    boolean firstResultOnly() {
        return limit != null && limit == 1;
    }

    private void appendPredicate(StringBuilder hql, String entityPath, PipelineYamlJpaPredicate predicate, ParameterCounter parameters) {
        String path = "e." + entityPath;
        switch (predicate.operator()) {
            case "eq" -> hql.append(path).append(" = :").append(parameters.next());
            case "in" -> hql.append(path).append(" in :").append(parameters.next());
            case "gt" -> hql.append(path).append(" > :").append(parameters.next());
            case "gte" -> hql.append(path).append(" >= :").append(parameters.next());
            case "lt" -> hql.append(path).append(" < :").append(parameters.next());
            case "lte" -> hql.append(path).append(" <= :").append(parameters.next());
            case "between" -> {
                hql.append(path)
                    .append(" between :")
                    .append(parameters.next())
                    .append(" and :")
                    .append(parameters.next());
            }
            case "like" -> hql.append(path).append(" like :").append(parameters.next());
            case "isNull" -> hql.append(path).append(Boolean.TRUE.equals(predicate.values().getFirst()) ? " is null" : " is not null");
            default -> throw new IllegalArgumentException("Unsupported JPA query operator: " + predicate.operator());
        }
    }

    private void bindPredicate(
        Map<String, Object> bindings,
        PipelineYamlJpaPredicate predicate,
        Object input,
        ParameterCounter parameters
    ) {
        switch (predicate.operator()) {
            case "isNull" -> {
                // Null checks are rendered directly and do not bind parameters.
            }
            case "between" -> {
                bindings.put(parameters.next(), resolveValue(input, predicate.values().get(0)));
                bindings.put(parameters.next(), resolveValue(input, predicate.values().get(1)));
            }
            case "in" -> bindings.put(parameters.next(), resolveInValue(input, predicate.values()));
            default -> bindings.put(parameters.next(), resolveValue(input, predicate.values().getFirst()));
        }
    }

    private Object resolveInValue(Object input, List<Object> values) {
        if (values.size() == 1) {
            Object value = resolveValue(input, values.getFirst());
            if (value instanceof Collection<?>) {
                return value;
            }
            if (value != null && value.getClass().isArray()) {
                int length = Array.getLength(value);
                List<Object> items = new ArrayList<>(length);
                for (int i = 0; i < length; i++) {
                    items.add(Array.get(value, i));
                }
                return List.copyOf(items);
            }
        }
        return values.stream()
            .map(value -> resolveValue(input, value))
            .toList();
    }

    private Object resolveValue(Object input, Object value) {
        if (value instanceof String text && text.startsWith(INPUT_PREFIX)) {
            return readRequiredInputValue(input, text);
        }
        return value;
    }

    private Object readRequiredInputValue(Object input, String inputReference) {
        if (input == null) {
            throw new IllegalArgumentException("query '" + queryId + "' input must not be null");
        }
        if (!inputReference.startsWith(INPUT_PREFIX)) {
            throw new IllegalArgumentException("query '" + queryId + "' where binding must start with input.: " + inputReference);
        }
        String property = inputReference.substring(INPUT_PREFIX.length());
        validateIdentifier(property, "input property");
        Object value = JpaQueryReflection.readProperty(input, property);
        if (value == null) {
            throw new IllegalArgumentException("query '" + queryId + "' input property '" + property + "' must not be null");
        }
        return value;
    }

    private static void validatePropertyMap(Map<String, String> values, String field) {
        values.forEach((key, value) -> {
            validatePath(key, "jpa." + field + " key");
            validatePath(value, "jpa." + field + " source");
        });
    }

    private static void validateOrderByMap(Map<String, String> values) {
        values.forEach((key, direction) -> {
            validatePath(key, "jpa.orderBy key");
            if (direction == null || !ORDER_DIRECTIONS.contains(direction.toLowerCase(Locale.ROOT))) {
                throw new IllegalArgumentException("jpa.orderBy direction must be asc or desc: " + direction);
            }
        });
    }

    private static void validateIdentifier(String value, String field) {
        if (value == null || !JAVA_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be a Java identifier: " + value);
        }
    }

    private static void validatePathMap(Map<String, PipelineYamlJpaPredicate> values, String field) {
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("jpa." + field + " must not be empty");
        }
        values.keySet().forEach(key -> validatePath(key, "jpa." + field + " key"));
    }

    private static void validatePath(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        for (String segment : value.split("\\.", -1)) {
            validateIdentifier(segment, field + " segment");
        }
    }

    private static Class<?> loadClass(String className) {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            return Class.forName(className, true, loader);
        } catch (ClassNotFoundException ex) {
            throw new IllegalArgumentException("JPA query entity class not found: " + className, ex);
        }
    }

    private static final class ParameterCounter {
        private int next;

        String next() {
            return "p" + next++;
        }
    }
}
