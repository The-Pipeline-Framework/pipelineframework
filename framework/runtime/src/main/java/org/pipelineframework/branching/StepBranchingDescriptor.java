package org.pipelineframework.branching;

import java.lang.reflect.Method;
import java.util.ArrayList;
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
    List<BranchVariantIdentity> inputVariants,
    List<BranchVariantIdentity> acceptedVariants,
    List<BranchVariantIdentity> producedVariants,
    boolean terminal
) {

    private static final ConcurrentHashMap<MethodCacheKey, List<VariantExtractor>> extractionCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<MethodCacheKey, List<VariantExtractor>> inputVariantExtractionCache = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Class<?>, Method[]> sortedMethodsCache = new ConcurrentHashMap<>();

    public StepBranchingDescriptor {
        acceptedContracts = acceptedContracts == null ? List.of() : List.copyOf(acceptedContracts);
        acceptedRuntimeClassNames = acceptedRuntimeClassNames == null ? List.of() : List.copyOf(acceptedRuntimeClassNames);
        acceptedRuntimeTypes = acceptedRuntimeTypes == null ? List.of() : List.copyOf(acceptedRuntimeTypes);
        inputVariants = inputVariants == null ? List.of() : List.copyOf(inputVariants);
        acceptedVariants = acceptedVariants == null ? List.of() : List.copyOf(acceptedVariants);
        producedVariants = producedVariants == null ? List.of() : List.copyOf(producedVariants);
    }

    public StepBranchingDescriptor(
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
        this(index, stepName, runtimeStepClass, inputRuntimeClassName, inputRuntimeType,
            acceptedContracts, acceptedRuntimeClassNames, acceptedRuntimeTypes,
            List.of(), List.of(), List.of(), terminal);
    }

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
        return extractAcceptedVariant(item)
            .map(this::wrapAcceptedVariant)
            .orElse(null);
    }

    /**
     * Resolves the declared union alternative carried by an item without using
     * that discriminator as a routing predicate.
     */
    public Optional<BranchVariantIdentity> variantIdentity(Object item) {
        if (item == null || inputVariants.isEmpty()) {
            return Optional.empty();
        }
        Optional<String> discriminator = discriminator(item);
        if (discriminator.isPresent()) {
            return inputVariants.stream()
                .filter(variant -> variant.discriminator().equals(discriminator.orElseThrow()))
                .findFirst();
        }
        return findInputVariantExtractor(item).flatMap(extractor -> inputVariants.stream()
            .filter(variant -> variant.discriminator().equals(extractor.discriminator()))
            .findFirst());
    }

    private Optional<Object> extractAcceptedVariant(Object item) {
        Class<?> itemClass = item.getClass();
        MethodCacheKey cacheKey = new MethodCacheKey(itemClass, this);
        List<VariantExtractor> extractors = extractionCache.computeIfAbsent(
            cacheKey,
            ignored -> findExtractors(itemClass));
        return extractors.stream()
            .map(candidate -> candidate.extract(item))
            .flatMap(Optional::stream)
            .filter(this::matchesAcceptedInstance)
            .findFirst();
    }

    private Optional<VariantExtractor> findInputVariantExtractor(Object item) {
        Class<?> itemClass = item.getClass();
        MethodCacheKey cacheKey = new MethodCacheKey(itemClass, this);
        List<VariantExtractor> extractors = inputVariantExtractionCache.computeIfAbsent(
            cacheKey,
            ignored -> findInputVariantExtractors(itemClass));
        return extractors.stream()
            .filter(extractor -> extractor.extract(item).isPresent())
            .findFirst();
    }

    private List<VariantExtractor> findInputVariantExtractors(Class<?> itemClass) {
        return Arrays.stream(getSortedMethods(itemClass))
            .filter(method -> method.getParameterCount() == 0 && method.getName().startsWith("get"))
            .filter(method -> !method.getName().substring("get".length()).isBlank())
            .filter(method -> method.getReturnType() != Void.TYPE)
            .filter(method -> inputVariants.stream().anyMatch(variant ->
                variant.discriminator().equals(decapitalize(method.getName().substring("get".length())))))
            .map(method -> variantExtractor(itemClass, method))
            .toList();
    }

    private VariantExtractor variantExtractor(Class<?> itemClass, Method getter) {
        String suffix = getter.getName().substring("get".length());
        getter.trySetAccessible();
        Optional<Method> hasMethod = findHasMethod(itemClass, suffix);
        hasMethod.ifPresent(Method::trySetAccessible);
        return new VariantExtractor(getter, hasMethod, decapitalize(suffix));
    }

    private List<VariantExtractor> findExtractors(Class<?> itemClass) {
        List<VariantExtractor> extractors = new ArrayList<>();
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
            extractors.add(variantExtractor(itemClass, method));
        }
        return List.copyOf(extractors);
    }

    private record VariantExtractor(Method getter, Optional<Method> hasMethod, String discriminator) {

        private Optional<Object> extract(Object item) {
            try {
                if (hasMethod.isPresent() && !Boolean.TRUE.equals(hasMethod.get().invoke(item))) {
                    return Optional.empty();
                }
                return Optional.ofNullable(getter.invoke(item));
            } catch (ReflectiveOperationException ignored) {
                return Optional.empty();
            }
        }
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
        return Arrays.stream(getSortedMethods(itemClass))
            .filter(method -> method.getName().equals("has" + suffix))
            .filter(method -> method.getParameterCount() == 0)
            .filter(method -> method.getReturnType() == Boolean.TYPE || method.getReturnType() == Boolean.class)
            .findFirst();
    }

    private static Optional<String> discriminator(Object item) {
        try {
            Optional<Method> candidate = Arrays.stream(getSortedMethods(item.getClass()))
                .filter(method -> method.getName().equals("discriminator"))
                .filter(method -> method.getParameterCount() == 0)
                .filter(method -> method.getReturnType() == String.class)
                .findFirst();
            if (candidate.isEmpty()) {
                return Optional.empty();
            }
            Method method = candidate.orElseThrow();
            method.trySetAccessible();
            Object value = method.invoke(item);
            return value instanceof String text && !text.isBlank() ? Optional.of(text) : Optional.empty();
        } catch (ReflectiveOperationException ignored) {
            return Optional.empty();
        }
    }

    private static String decapitalize(String value) {
        return value.isEmpty() ? value : Character.toLowerCase(value.charAt(0)) + value.substring(1);
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
