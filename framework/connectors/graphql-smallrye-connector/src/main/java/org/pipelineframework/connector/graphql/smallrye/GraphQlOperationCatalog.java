package org.pipelineframework.connector.graphql.smallrye;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import graphql.language.OperationDefinition;
import graphql.parser.Parser;

/** Loads and validates a digest-pinned operation allowlist without contacting an endpoint. */
final class GraphQlOperationCatalog {
    private GraphQlOperationCatalog() {
    }

    static Map<String, LinkedOperation> load(
        GraphQlProviderConfiguration configuration,
        ClassLoader classLoader
    ) {
        Objects.requireNonNull(configuration, "GraphQL configuration must not be null");
        Objects.requireNonNull(classLoader, "GraphQL resource classloader must not be null");
        Map<String, LinkedOperation> linked = new LinkedHashMap<>();
        configuration.operations().forEach((key, declared) -> linked.put(key, load(key, declared, classLoader)));
        return Map.copyOf(linked);
    }

    private static LinkedOperation load(
        String key,
        GraphQlPersistedOperation declared,
        ClassLoader classLoader
    ) {
        byte[] bytes = resource(declared.resource(), classLoader);
        String actualDigest = digest(bytes);
        if (!declared.sha256().equals(actualDigest)) {
            throw new IllegalArgumentException("GraphQL operation '" + key + "' resource '" + declared.resource()
                + "' has SHA-256 " + actualDigest + " but the binding declares " + declared.sha256());
        }
        String document = new String(bytes, StandardCharsets.UTF_8);
        List<OperationDefinition> operations;
        try {
            operations = Parser.parse(document).getDefinitionsOfType(OperationDefinition.class);
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException("GraphQL operation '" + key + "' resource could not be parsed", failure);
        }
        if (operations.size() != 1) {
            throw new IllegalArgumentException("GraphQL operation '" + key
                + "' resource must contain exactly one operation definition but contained " + operations.size());
        }
        OperationDefinition operation = operations.getFirst();
        if (!declared.operationName().equals(operation.getName())) {
            throw new IllegalArgumentException("GraphQL operation '" + key + "' resource declares operation '"
                + operation.getName() + "' rather than '" + declared.operationName() + "'");
        }
        GraphQlOperationKind parsedKind = switch (operation.getOperation()) {
            case QUERY -> GraphQlOperationKind.QUERY;
            case MUTATION -> GraphQlOperationKind.MUTATION;
            case SUBSCRIPTION -> throw new IllegalArgumentException(
                "GraphQL operation '" + key + "' subscriptions are not supported");
        };
        if (parsedKind != declared.kind()) {
            throw new IllegalArgumentException("GraphQL operation '" + key + "' is " + parsedKind
                + " but the binding declares " + declared.kind());
        }
        return new LinkedOperation(parsedKind, declared.operationName(), document);
    }

    private static byte[] resource(String resource, ClassLoader classLoader) {
        try (InputStream input = classLoader.getResourceAsStream(resource)) {
            if (input == null) throw new IllegalArgumentException("GraphQL operation resource not found: " + resource);
            return input.readAllBytes();
        } catch (IOException exception) {
            throw new IllegalArgumentException("GraphQL operation resource could not be read: " + resource, exception);
        }
    }

    private static String digest(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    record LinkedOperation(GraphQlOperationKind kind, String operationName, String document) {
        LinkedOperation {
            kind = Objects.requireNonNull(kind, "linked GraphQL operation kind must not be null");
            operationName = Objects.requireNonNull(operationName, "linked GraphQL operation name must not be null");
            document = Objects.requireNonNull(document, "linked GraphQL document must not be null");
        }
    }
}
