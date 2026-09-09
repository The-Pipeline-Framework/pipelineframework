package org.pipelineframework.blocks.graphql.agent;

import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

/** One persisted-operation key described to the model without granting execution authority. */
public record GraphQlAgentOperation(String operationKey, String kind, String description, String variablesGuide) {
    private static final Pattern KEY = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");

    public GraphQlAgentOperation {
        operationKey = required(operationKey, "operationKey");
        if (!KEY.matcher(operationKey).matches()) {
            throw new IllegalArgumentException("GraphQL agent operationKey has invalid syntax: " + operationKey);
        }
        kind = required(kind, "kind").toUpperCase(Locale.ROOT);
        if (!kind.equals("QUERY") && !kind.equals("MUTATION")) {
            throw new IllegalArgumentException("GraphQL agent operation kind must be QUERY or MUTATION");
        }
        description = bounded(description, "description", 1_024);
        variablesGuide = bounded(variablesGuide, "variablesGuide", 2_048);
    }

    private static String required(String value, String field) {
        return bounded(value, field, 128);
    }

    private static String bounded(String value, String field, int maximum) {
        String checked = Objects.requireNonNull(value, "GraphQL agent " + field + " must not be null").trim();
        if (checked.isEmpty()) {
            throw new IllegalArgumentException("GraphQL agent " + field + " must not be blank");
        }
        if (checked.length() > maximum) {
            throw new IllegalArgumentException("GraphQL agent " + field + " must not exceed " + maximum + " characters");
        }
        return checked;
    }
}
