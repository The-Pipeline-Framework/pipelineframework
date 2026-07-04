package org.pipelineframework.branching;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Runtime applicability descriptor for one step in a branch-aware pipeline.
 */
public record StepBranchingDescriptor(
    int index,
    String stepName,
    String runtimeStepClass,
    String inputRuntimeClassName,
    Class<?> inputRuntimeType,
    List<String> acceptedContracts,
    List<String> acceptedRuntimeClassNames,
    List<Class<?>> acceptedRuntimeTypes,
    boolean terminal
) {

    public boolean accepts(Object item) {
        return applicableItem(item) != null;
    }

    public Object applicableItem(Object item) {
        if (item == null) {
            return null;
        }
        if (matchesAcceptedInstance(item)) {
            return wrapAcceptedVariant(item);
        }
        return extractAcceptedVariant(item);
    }

    private Object extractAcceptedVariant(Object item) {
        for (Method method : item.getClass().getMethods()) {
            if (method.getParameterCount() != 0 || !method.getName().startsWith("get")) {
                continue;
            }
            String suffix = method.getName().substring("get".length());
            if (suffix.isBlank()) {
                continue;
            }
            Class<?> returnType = method.getReturnType();
            if (returnType == Void.TYPE) {
                continue;
            }
            boolean returnTypeAccepted = matchesAcceptedType(returnType);
            if (!returnTypeAccepted) {
                continue;
            }
            Method hasMethod = findHasMethod(item.getClass(), suffix);
            try {
                if (hasMethod != null) {
                    hasMethod.trySetAccessible();
                    if (!Boolean.TRUE.equals(hasMethod.invoke(item))) {
                        continue;
                    }
                }
                method.trySetAccessible();
                Object candidate = method.invoke(item);
                if (candidate == null) {
                    continue;
                }
                if (matchesAcceptedInstance(candidate)) {
                    return candidate;
                }
            } catch (ReflectiveOperationException ignored) {
                // Fall through to the next candidate getter.
            }
        }
        return null;
    }

    private Object wrapAcceptedVariant(Object item) {
        if (inputRuntimeType == null || inputRuntimeType.isInstance(item)) {
            return item;
        }
        Object wrapped = wrapWithBuilder(item);
        return wrapped != null ? wrapped : item;
    }

    private Object wrapWithBuilder(Object item) {
        try {
            Method newBuilder = inputRuntimeType.getMethod("newBuilder");
            newBuilder.trySetAccessible();
            Object builder = newBuilder.invoke(null);
            for (Method method : builder.getClass().getMethods()) {
                if (method.getParameterCount() != 1 || !method.getName().startsWith("set")) {
                    continue;
                }
                Class<?> parameterType = method.getParameterTypes()[0];
                if (!matchesAcceptedType(parameterType) || !parameterType.isAssignableFrom(item.getClass())) {
                    continue;
                }
                method.trySetAccessible();
                method.invoke(builder, item);
                Method build = builder.getClass().getMethod("build");
                build.trySetAccessible();
                Object wrapped = build.invoke(builder);
                if (inputRuntimeType.isInstance(wrapped)) {
                    return wrapped;
                }
            }
        } catch (ReflectiveOperationException ignored) {
            // Not a builder-backed wrapper type; fall back to the concrete item.
        }
        return null;
    }

    private static Method findHasMethod(Class<?> itemClass, String suffix) {
        try {
            return itemClass.getMethod("has" + suffix);
        } catch (NoSuchMethodException e) {
            return null;
        }
    }

    private boolean matchesAcceptedInstance(Object candidate) {
        return candidate != null && matchesAcceptedType(candidate.getClass());
    }

    private boolean matchesAcceptedType(Class<?> runtimeType) {
        String runtimeTypeName = runtimeType.getName();
        for (int i = 0; i < acceptedRuntimeClassNames.size(); i++) {
            String acceptedName = acceptedRuntimeClassNames.get(i);
            if (acceptedName.equals(runtimeTypeName)) {
                return true;
            }
            if (i < acceptedRuntimeTypes.size() && acceptedRuntimeTypes.get(i).isAssignableFrom(runtimeType)) {
                return true;
            }
        }
        return false;
    }
}
