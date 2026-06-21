package org.pipelineframework.connector.query.jpa;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.pipelineframework.config.pipeline.PipelineYamlJpaQuery;
import org.pipelineframework.query.QueryStepDescriptor;

final class JpaQueryPlan {
    private static final Pattern JAVA_IDENTIFIER = Pattern.compile("[A-Za-z_$][A-Za-z\\d_$]*");
    private static final String INPUT_PREFIX = "input.";

    private final String queryId;
    private final Class<?> entityType;
    private final Map<String, String> where;
    private final Map<String, String> projection;

    private JpaQueryPlan(String queryId, Class<?> entityType, Map<String, String> where, Map<String, String> projection) {
        this.queryId = queryId;
        this.entityType = entityType;
        this.where = Map.copyOf(where);
        this.projection = Map.copyOf(projection);
    }

    static JpaQueryPlan from(QueryStepDescriptor descriptor) {
        PipelineYamlJpaQuery jpa = descriptor.jpa();
        Class<?> entityType = loadClass(jpa.entity());
        validatePropertyMap(jpa.where(), "where");
        validatePropertyMap(jpa.projection(), "projection");
        return new JpaQueryPlan(descriptor.queryId(), entityType, jpa.where(), jpa.projection());
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
        for (String entityField : where.keySet()) {
            if (index > 0) {
                hql.append(" and ");
            }
            hql.append("e.").append(entityField).append(" = :p").append(index);
            index++;
        }
        return hql.toString();
    }

    Map<String, Object> bindings(Object input) {
        Map<String, Object> bindings = new LinkedHashMap<>();
        int index = 0;
        for (String inputReference : where.values()) {
            Object value = readRequiredInputValue(input, inputReference);
            bindings.put("p" + index, value);
            index++;
        }
        return Map.copyOf(bindings);
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
            validateIdentifier(key, "jpa." + field + " key");
            if ("where".equals(field)) {
                if (!value.startsWith(INPUT_PREFIX)) {
                    throw new IllegalArgumentException("jpa.where binding must start with input.: " + value);
                }
                validateIdentifier(value.substring(INPUT_PREFIX.length()), "jpa.where input property");
            } else {
                validateIdentifier(value, "jpa.projection source");
            }
        });
    }

    private static void validateIdentifier(String value, String field) {
        if (value == null || !JAVA_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException(field + " must be a Java identifier: " + value);
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
}
