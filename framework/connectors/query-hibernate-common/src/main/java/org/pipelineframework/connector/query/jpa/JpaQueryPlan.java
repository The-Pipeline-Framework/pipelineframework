package org.pipelineframework.connector.query.jpa;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

final class JpaQueryPlan {
    private static final Pattern JAVA_IDENTIFIER = Pattern.compile("[A-Za-z_$][A-Za-z\\d_$]*");
    private static final String INPUT_PREFIX = "input.";
    private static final Set<String> ORDER_DIRECTIONS = Set.of("asc", "desc");

    private final String queryId;
    private final Class<?> entityType;
    private final Map<String, JpaPredicate> where;
    private final Map<String, String> projection;
    private final Map<String, String> orderBy;
    private final Optional<Integer> limit;

    private JpaQueryPlan(
        String queryId,
        Class<?> entityType,
        Map<String, JpaPredicate> where,
        Map<String, String> projection,
        Map<String, String> orderBy,
        Optional<Integer> limit
    ) {
        this.queryId = queryId;
        this.entityType = entityType;
        this.where = Collections.unmodifiableMap(new LinkedHashMap<>(where));
        this.projection = Collections.unmodifiableMap(new LinkedHashMap<>(projection));
        this.orderBy = Collections.unmodifiableMap(new LinkedHashMap<>(orderBy));
        this.limit = limit;
    }

    static JpaQueryPlan from(String queryId, JpaFindOneConfiguration configuration) {
        return fromOne(
            queryId,
            configuration.entity(),
            configuration.where(),
            configuration.projection().orElse(Map.of()),
            configuration.orderBy().orElse(Map.of()),
            configuration.limit(),
            configuration.result().orElse("single"));
    }

    static JpaQueryPlan fromMany(String queryId, JpaFindManyConfiguration configuration) {
        String entity = configuration.entity();
        Map<String, JpaPredicate> where = configuration.where();
        Map<String, String> projection = configuration.projection().orElse(Map.of());
        Map<String, String> orderBy = configuration.orderBy();
        List<String> uniqueBy = configuration.uniqueBy();
        Optional<Integer> limit = configuration.limit();

        validateCommon(entity, where, projection, orderBy);
        validateTotalOrder(orderBy, uniqueBy);
        if (limit.isPresent() && limit.orElseThrow() <= 0) {
            throw new IllegalArgumentException("query jpa.limit must be positive");
        }
        return new JpaQueryPlan(queryId, loadClass(entity), where, projection, orderBy, limit);
    }

    private static JpaQueryPlan fromOne(
        String queryId,
        String entity,
        Map<String, JpaPredicate> where,
        Map<String, String> projection,
        Map<String, String> orderBy,
        Optional<Integer> limit,
        String result
    ) {
        validateCommon(entity, where, projection, orderBy);
        if (limit.isPresent() && limit.orElseThrow() != 1) {
            throw new IllegalArgumentException("query jpa.limit supports only 1 in v2");
        }
        if (limit.isPresent() && orderBy.isEmpty()) {
            throw new IllegalArgumentException("query jpa.limit requires orderBy");
        }
        if (!"single".equals(result)) {
            throw new IllegalArgumentException("query jpa.result supports only single in v1");
        }
        return new JpaQueryPlan(queryId, loadClass(entity), where, projection, orderBy, limit);
    }

    private static void validateCommon(
        String entity,
        Map<String, JpaPredicate> where,
        Map<String, String> projection,
        Map<String, String> orderBy
    ) {
        if (entity == null || entity.isBlank()) {
            throw new IllegalArgumentException("query jpa.entity must not be blank");
        }
        validatePathMap(where, "where");
        validatePropertyMap(projection, "projection");
        validateOrderByMap(orderBy);
    }

    private static void validateTotalOrder(Map<String, String> orderBy, List<String> uniqueBy) {
        if (orderBy.isEmpty()) {
            throw new IllegalArgumentException("streaming query jpa.orderBy must not be empty");
        }
        if (uniqueBy.isEmpty()) {
            throw new IllegalArgumentException("streaming query jpa.uniqueBy must not be empty");
        }
        uniqueBy.forEach(path -> validatePath(path, "jpa.uniqueBy value"));
        if (new HashSet<>(uniqueBy).size() != uniqueBy.size()) {
            throw new IllegalArgumentException("streaming query jpa.uniqueBy must not contain duplicates");
        }
        List<String> orderedPaths = List.copyOf(orderBy.keySet());
        if (uniqueBy.size() > orderedPaths.size()
            || !orderedPaths.subList(orderedPaths.size() - uniqueBy.size(), orderedPaths.size()).equals(uniqueBy)) {
            throw new IllegalArgumentException("streaming query jpa.uniqueBy must be an ordered suffix of orderBy");
        }
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
        for (Map.Entry<String, JpaPredicate> entry : where.entrySet()) {
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
        for (JpaPredicate predicate : where.values()) {
            bindPredicate(bindings, predicate, input, parameters);
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(bindings));
    }

    int maxResults() {
        return limit.filter(value -> value == 1).isPresent() ? 1 : 2;
    }

    boolean firstResultOnly() {
        return limit.filter(value -> value == 1).isPresent();
    }

    Optional<Integer> streamingLimit() {
        return limit;
    }

    OrderingGuard orderingGuard() {
        return new OrderingGuard(List.copyOf(orderBy.keySet()));
    }

    private void appendPredicate(StringBuilder hql, String entityPath, JpaPredicate predicate, ParameterCounter parameters) {
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
        JpaPredicate predicate,
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
        return JpaQueryReflection.readProperty(input, property)
            .orElseThrow(() -> new IllegalArgumentException(
                "query '" + queryId + "' input property '" + property + "' must not be null"));
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

    private static void validatePathMap(Map<String, JpaPredicate> values, String field) {
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

    static final class OrderingGuard {
        private static final Object NULL_ORDER_VALUE = new Object();
        private final List<String> paths;
        private List<Object> previous;

        private OrderingGuard(List<String> paths) {
            this.paths = paths;
        }

        void validateNext(Object entity) {
            List<Object> current = paths.stream()
                .map(path -> JpaQueryReflection.readProperty(entity, path).orElse(NULL_ORDER_VALUE))
                .toList();
            if (previous != null && previous.equals(current)) {
                throw new IllegalStateException(
                    "Hibernate streaming query produced adjacent rows with the same orderBy tuple");
            }
            previous = current;
        }
    }
}
