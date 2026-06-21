package org.pipelineframework.connector.query.jpa;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

final class JpaQueryReflection {
    private JpaQueryReflection() {
    }

    static Object readProperty(Object target, String property) {
        Method accessor = findAccessor(target.getClass(), property);
        if (accessor != null) {
            try {
                accessor.setAccessible(true);
                return accessor.invoke(target);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException("Failed to read property '" + property + "' from " + target.getClass().getName(), ex);
            }
        }
        Field field = findField(target.getClass(), property);
        if (field != null) {
            try {
                field.setAccessible(true);
                return field.get(target);
            } catch (ReflectiveOperationException ex) {
                throw new IllegalStateException("Failed to read field '" + property + "' from " + target.getClass().getName(), ex);
            }
        }
        throw new IllegalArgumentException("Property '" + property + "' not found on " + target.getClass().getName());
    }

    private static Method findAccessor(Class<?> type, String property) {
        for (String candidate : accessorNames(property)) {
            try {
                return type.getMethod(candidate);
            } catch (NoSuchMethodException ignored) {
                // try the next JavaBean or record-style accessor name
            }
        }
        return null;
    }

    private static String[] accessorNames(String property) {
        String capitalized = Character.toUpperCase(property.charAt(0)) + property.substring(1);
        return new String[] { property, "get" + capitalized, "is" + capitalized };
    }

    private static Field findField(Class<?> type, String property) {
        Class<?> current = type;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(property);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }
}
