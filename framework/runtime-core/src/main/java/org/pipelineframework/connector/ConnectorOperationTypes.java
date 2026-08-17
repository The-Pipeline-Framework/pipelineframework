package org.pipelineframework.connector;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** Framework-owned projection of executable generic signatures into static semantic metadata. */
final class ConnectorOperationTypes {
    private ConnectorOperationTypes() {
    }

    static Optional<ConnectorOperationTypeContract> contract(ConnectorOperation operation) {
        Class<?> family = operation instanceof CommandOperation<?, ?, ?>
            ? CommandOperation.class
            : operation instanceof QueryOperation<?, ?, ?> ? QueryOperation.class : null;
        if (family == null) {
            return Optional.empty();
        }
        return findArguments(operation.getClass(), family)
            .map(arguments -> new ConnectorOperationTypeContract(
                normalize(arguments[0]), output(arguments[2])));
    }

    private static Optional<String> output(Type type) {
        if (type == Void.class || type == void.class) {
            return Optional.empty();
        }
        return Optional.of(normalize(type));
    }

    private static Optional<Type[]> findArguments(Type candidate, Class<?> family) {
        if (candidate instanceof ParameterizedType parameterized) {
            if (parameterized.getRawType().equals(family)) {
                return Optional.of(parameterized.getActualTypeArguments());
            }
            return findArguments(parameterized.getRawType(), family);
        }
        if (!(candidate instanceof Class<?> type)) {
            return Optional.empty();
        }
        for (Type implemented : type.getGenericInterfaces()) {
            Optional<Type[]> found = findArguments(implemented, family);
            if (found.isPresent()) {
                return found;
            }
        }
        Type parent = type.getGenericSuperclass();
        return parent == null ? Optional.empty() : findArguments(parent, family);
    }

    private static String normalize(Type type) {
        if (type instanceof Class<?> raw) {
            if (raw.isArray()) {
                return "list<" + normalize(raw.getComponentType()) + ">";
            }
            if (raw == String.class || raw == Character.class || raw == char.class) {
                return "string";
            }
            if (raw == Boolean.class || raw == boolean.class) {
                return "boolean";
            }
            if (raw == Byte.class || raw == byte.class || raw == Short.class || raw == short.class
                || raw == Integer.class || raw == int.class || raw == Long.class || raw == long.class
                || raw == BigInteger.class) {
                return "integer";
            }
            if (raw == Float.class || raw == float.class || raw == Double.class || raw == double.class
                || raw == BigDecimal.class) {
                return "number";
            }
            return raw.getName();
        }
        if (type instanceof ParameterizedType parameterized) {
            Type raw = parameterized.getRawType();
            Type[] arguments = parameterized.getActualTypeArguments();
            if (raw instanceof Class<?> rawClass && Collection.class.isAssignableFrom(rawClass) && arguments.length == 1) {
                return "list<" + normalize(arguments[0]) + ">";
            }
            if (raw instanceof Class<?> rawClass && Map.class.isAssignableFrom(rawClass) && arguments.length == 2) {
                return "map<" + normalize(arguments[0]) + "," + normalize(arguments[1]) + ">";
            }
            StringBuilder value = new StringBuilder(normalize(raw)).append('<');
            for (int index = 0; index < arguments.length; index++) {
                if (index > 0) {
                    value.append(',');
                }
                value.append(normalize(arguments[index]));
            }
            return value.append('>').toString();
        }
        if (type instanceof GenericArrayType array) {
            return "list<" + normalize(array.getGenericComponentType()) + ">";
        }
        if (type instanceof TypeVariable<?> variable) {
            throw new IllegalArgumentException("connector operation type is unresolved: " + variable.getName());
        }
        if (type instanceof WildcardType) {
            throw new IllegalArgumentException("connector operation type must not use a wildcard: " + type.getTypeName());
        }
        throw new IllegalArgumentException("unsupported connector operation type: " + type.getTypeName());
    }
}
