package org.pipelineframework.connector.query.jpa;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Optional;

final class JpaQueryReflection {
    private JpaQueryReflection() {
    }

    static Object readProperty(Object target, String property) {
        if (property != null && property.contains(".")) {
            return readPath(target, property);
        }
        Optional<Method> accessor = findAccessor(target.getClass(), property);
        if (accessor.isPresent()) {
            return invokeAccessor(accessor.get(), target, property);
        }
        Optional<Field> field = findField(target.getClass(), property);
        if (field.isPresent()) {
            return readField(field.get(), target, property);
        }
        throw new IllegalArgumentException(
            "Property '" + property + "' not found on " + target.getClass().getName());
    }

    private static Object readPath(Object target, String path) {
        Object current = target;
        for (String segment : path.split("\\.")) {
            if (current == null) {
                throw new IllegalArgumentException("Property path '" + path + "' resolved through null segment '" + segment + "'");
            }
            current = readProperty(current, segment);
        }
        return current;
    }

    private static Optional<Method> findAccessor(Class<?> type, String property) {
        for (String candidate : accessorNames(property)) {
            try {
                return Optional.of(type.getMethod(candidate));
            } catch (NoSuchMethodException ignored) {
                // try the next JavaBean or record-style accessor name
            }
        }
        return Optional.empty();
    }

    private static String[] accessorNames(String property) {
        String capitalized = Character.toUpperCase(property.charAt(0)) + property.substring(1);
        return new String[] { property, "get" + capitalized, "is" + capitalized };
    }

    private static Optional<Field> findField(Class<?> type, String property) {
        Class<?> current = type;
        while (current != null && current != Object.class) {
            try {
                return Optional.of(current.getDeclaredField(property));
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return Optional.empty();
    }

    private static Object invokeAccessor(Method accessor, Object target, String property) {
        try {
            accessor.setAccessible(true);
            return accessor.invoke(target);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Failed to read property '" + property + "' from " + target.getClass().getName(), ex);
        }
    }

    private static Object readField(Field field, Object target, String property) {
        try {
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Failed to read field '" + property + "' from " + target.getClass().getName(), ex);
        }
    }
}
