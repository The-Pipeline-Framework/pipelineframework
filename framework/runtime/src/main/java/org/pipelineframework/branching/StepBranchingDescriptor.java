package org.pipelineframework.branching;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final ConcurrentHashMap<MethodCacheKey, Optional<Object>> extractionCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, Method[]> sortedMethodsCache = new ConcurrentHashMap<>();

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
        return extractAcceptedVariant(item).orElse(null);
    }

    private Optional<Object> extractAcceptedVariant(Object item) {
        Class<?> itemClass = item.getClass();
        MethodCacheKey cacheKey = new MethodCacheKey(itemClass, this);
        Optional<Object> cached = extractionCache.get(cacheKey);
        if (cached != null) {
            return cached;
        }
        Method[] methods = getSortedMethods(itemClass);
        for (Method method : methods) {
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
            Optional<Method> hasMethod = findHasMethod(itemClass, suffix);
            try {
                if (hasMethod.isPresent()) {
                    Method has = hasMethod.get();
                    has.trySetAccessible();
                    if (!Boolean.TRUE.equals(has.invoke(item))) {
                        continue;
                    }
                }
                method.trySetAccessible();
                Object candidate = method.invoke(item);
                if (candidate == null) {
                    continue;
                }
                if (matchesAcceptedInstance(candidate)) {
                    Optional<Object> result = Optional.of(candidate);
                    extractionCache.put(cacheKey, result);
                    return result;
                }
            } catch (ReflectiveOperationException ignored) {
                // Fall through to the next candidate getter.
            }
        }
        Optional<Object> result = Optional.empty();
        extractionCache.put(cacheKey, result);
        return result;
    }

    private Object wrapAcceptedVariant(Object item) {
        if (inputRuntimeType == null || inputRuntimeType.isInstance(item)) {
            return item;
        }
        return wrapWithBuilder(item).orElse(item);
    }

    private Optional<Object> wrapWithBuilder(Object item) {
        try {
            Method newBuilder = inputRuntimeType.getMethod("newBuilder");
            newBuilder.trySetAccessible();
            Object builder = newBuilder.invoke(null);
            Method[] methods = getSortedMethods(builder.getClass());
            for (Method method : methods) {
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
                    return Optional.of(wrapped);
                }
            }
        } catch (ReflectiveOperationException ignored) {
            // Not a builder-backed wrapper type; fall back to the concrete item.
        }
        return Optional.empty();
    }

    private static Optional<Method> findHasMethod(Class<?> itemClass, String suffix) {
        try {
            return Optional.of(itemClass.getMethod("has" + suffix));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
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

    private static Method[] getSortedMethods(Class<?> clazz) {
        return sortedMethodsCache.computeIfAbsent(clazz, c -> {
            Method[] methods = c.getMethods();
            Arrays.sort(methods, Comparator.comparing(Method::getName)
                .thenComparing(m -> m.getParameterCount())
                .thenComparing(m -> m.getReturnType().getName()));
            return methods;
        });
    }

    private record MethodCacheKey(Class<?> itemClass, StepBranchingDescriptor descriptor) {
    }
}
