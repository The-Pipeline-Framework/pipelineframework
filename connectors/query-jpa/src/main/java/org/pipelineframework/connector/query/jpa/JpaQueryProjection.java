package org.pipelineframework.connector.query.jpa;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.Map;

final class JpaQueryProjection {
    private JpaQueryProjection() {
    }

    static <O> O project(Object entity, Class<O> outputType, Map<String, String> projection) {
        if (entity == null) {
            throw new IllegalArgumentException("query entity row must not be null");
        }
        if (!outputType.isRecord()) {
            throw new IllegalArgumentException("JPA query output must be a Java record in v1: " + outputType.getName());
        }
        try {
            RecordComponent[] components = outputType.getRecordComponents();
            Class<?>[] parameterTypes = new Class<?>[components.length];
            Object[] arguments = new Object[components.length];
            for (int i = 0; i < components.length; i++) {
                RecordComponent component = components[i];
                parameterTypes[i] = component.getType();
                String sourceProperty = projection.getOrDefault(component.getName(), component.getName());
                arguments[i] = JpaQueryReflection.readProperty(entity, sourceProperty);
            }
            Constructor<O> constructor = outputType.getDeclaredConstructor(parameterTypes);
            constructor.setAccessible(true);
            return constructor.newInstance(arguments);
        } catch (ReflectiveOperationException ex) {
            throw new IllegalStateException("Failed to project JPA query output as " + outputType.getName(), ex);
        }
    }
}
