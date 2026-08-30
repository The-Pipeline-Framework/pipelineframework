package org.pipelineframework.awaitable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;

import io.smallrye.mutiny.Uni;
import org.pipelineframework.config.pipeline.PipelineYamlConfig;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLoader;
import org.pipelineframework.config.pipeline.PipelineYamlConfigLocator;
import org.pipelineframework.config.pipeline.PipelineYamlDocumentLoader;
import org.pipelineframework.config.pipeline.PipelineYamlAwaitCompletion;
import org.pipelineframework.config.pipeline.PipelineYamlStep;
import org.pipelineframework.config.template.PipelineTemplateConfig;
import org.pipelineframework.config.template.PipelineTemplateConfigLoader;
import org.pipelineframework.config.template.PipelineTemplateStep;

/**
 * Builds await descriptors from runtime pipeline YAML.
 */
@ApplicationScoped
public class AwaitStepDescriptorFactory {
    private static final int DESCRIPTOR_LOADER_THREADS = Math.max(2, Runtime.getRuntime().availableProcessors());
    private static final int DESCRIPTOR_LOADER_QUEUE_SIZE = 256;

    private final Map<String, AwaitStepDescriptor> descriptors = new ConcurrentHashMap<>();
    private final AtomicInteger threadCounter = new AtomicInteger();
    private final ExecutorService blockingExecutor = new ThreadPoolExecutor(
        1,
        DESCRIPTOR_LOADER_THREADS,
        60L,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(DESCRIPTOR_LOADER_QUEUE_SIZE),
        r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("await-descriptor-loader-" + threadCounter.incrementAndGet());
            return t;
        });

    /**
     * Resolves the descriptor for a generated await step.
     */
    public Uni<AwaitStepDescriptor> descriptor(String serviceName, String inputType, String outputType) {
        AwaitStepDescriptor cached = descriptors.get(serviceName);
        if (cached != null) {
            return Uni.createFrom().item(cached)
                .onItem().invoke(resolved -> ensureCompatible(
                    resolved, inputType, outputType, resolved.transportInputType(), resolved.transportOutputType()));
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(
                serviceName,
                key -> loadCanonicalDescriptor(key, inputType, outputType)))
            .onItem().invoke(resolved -> ensureCompatible(
                resolved, inputType, outputType, resolved.transportInputType(), resolved.transportOutputType()))
            .runSubscriptionOn(blockingExecutor);
    }

    public Uni<AwaitStepDescriptor> descriptor(
        String serviceName,
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType
    ) {
        AwaitStepDescriptor cached = descriptors.get(serviceName);
        if (cached != null) {
            return Uni.createFrom().item(cached)
                .onItem().invoke(resolved -> ensureCompatible(
                    resolved, inputType, outputType, transportInputType, transportOutputType));
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(serviceName,
                key -> loadDescriptor(
                    key,
                    inputType,
                    outputType,
                    transportInputType,
                    transportOutputType,
                    Function.identity(),
                    Function.identity())))
            .onItem().invoke(resolved -> ensureCompatible(
                resolved,
                inputType,
                outputType,
                transportInputType,
                transportOutputType))
            .runSubscriptionOn(blockingExecutor);
    }

    /**
     * Resolves and registers a generated canonical-to-transport boundary.
     *
     * <p>The functions are runtime-only generated adapters. Durable interactions retain only
     * stable type identities; on replay the descriptor is rebuilt by its stable step id.</p>
     */
    public Uni<AwaitStepDescriptor> descriptor(
        String serviceName,
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType,
        Function<Object, Object> inputToTransport,
        Function<Object, Object> outputFromTransport
    ) {
        AwaitStepDescriptor cached = descriptors.get(serviceName);
        if (cached != null) {
            return Uni.createFrom().item(cached)
                .onItem().invoke(resolved -> ensureCompatible(
                    resolved, inputType, outputType, transportInputType, transportOutputType));
        }
        return Uni.createFrom()
            .item(() -> descriptors.computeIfAbsent(serviceName,
                key -> loadDescriptor(
                    key,
                    inputType,
                    outputType,
                    transportInputType,
                    transportOutputType,
                    inputToTransport,
                    outputFromTransport)))
            .onItem().invoke(resolved -> ensureCompatible(
                resolved,
                inputType,
                outputType,
                transportInputType,
                transportOutputType))
            .runSubscriptionOn(blockingExecutor);
    }

    /**
     * Registers a descriptor constructed by a direct runtime caller so completion and replay can
     * resolve it from the durable interaction's stable step id.
     */
    public AwaitStepDescriptor register(AwaitStepDescriptor descriptor) {
        if (descriptor == null) {
            throw new IllegalArgumentException("descriptor must not be null");
        }
        AwaitStepDescriptor existing = descriptors.putIfAbsent(descriptor.stepId(), descriptor);
        if (existing == null) {
            return descriptor;
        }
        ensureCompatible(
            existing,
            descriptor.inputType(),
            descriptor.outputType(),
            descriptor.transportInputType(),
            descriptor.transportOutputType());
        return existing;
    }

    /**
     * Resolves a descriptor for a durable interaction. The interaction never persisted input
     * identities, so they must come from this rebuilt descriptor rather than be guessed.
     */
    public Uni<AwaitStepDescriptor> descriptorByStepId(String stepId) {
        return Uni.createFrom()
            .item(() -> descriptorByStepIdNow(stepId))
            .runSubscriptionOn(blockingExecutor);
    }

    public AwaitStepDescriptor descriptorByStepIdNow(String stepId) {
        if (stepId == null || stepId.isBlank()) {
            throw new IllegalArgumentException("stepId must not be blank");
        }
        return descriptors.computeIfAbsent(stepId, this::loadLegacyDescriptor);
    }

    @PreDestroy
    void shutdown() {
        blockingExecutor.shutdown();
    }

    private AwaitStepDescriptor loadDescriptor(
        String serviceName,
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType,
        Function<Object, Object> inputToTransport,
        Function<Object, Object> outputFromTransport
    ) {
        PipelineYamlStep step = awaitStep(serviceName);
        return descriptorForStep(
            serviceName,
            step,
            inputType,
            outputType,
            transportInputType,
            transportOutputType,
            inputToTransport,
            outputFromTransport);
    }

    /**
     * Rebuilds the generated representation boundary for the historical three-argument generated
     * client call. Version 3 clients use canonical domain types at the step boundary, so assuming
     * the transport types are identical loses a protobuf union arm before durable completion can
     * be admitted.
     */
    private AwaitStepDescriptor loadCanonicalDescriptor(String serviceName, String inputType, String outputType) {
        Path configPath = resolveConfigPath(serviceName);
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        PipelineYamlStep step = awaitStep(config, serviceName);
        Optional<V3AwaitTypeBinding> v3Binding = generatedV3TypeBinding(
            configPath,
            serviceName,
            inputType,
            outputType);
        if (v3Binding.isPresent()) {
            V3AwaitTypeBinding binding = v3Binding.get();
            validateCanonicalTypes(serviceName, inputType, outputType, binding);
            return descriptorForStep(
                serviceName,
                step,
                binding.inputType(),
                binding.outputType(),
                binding.transportInputType(),
                binding.transportOutputType(),
                binding.inputToTransport(),
                binding.outputFromTransport());
        }
        return descriptorForStep(
            serviceName,
            step,
            inputType,
            outputType,
            inputType,
            outputType,
            Function.identity(),
            Function.identity());
    }

    private static void validateCanonicalTypes(
        String serviceName,
        String inputType,
        String outputType,
        V3AwaitTypeBinding binding
    ) {
        if (!binding.inputType().equals(inputType) || !binding.outputType().equals(outputType)) {
            throw new IllegalStateException(
                "Version 3 await step " + serviceName + " must use its generated canonical types "
                    + binding.inputType() + " -> " + binding.outputType() + " but received "
                    + inputType + " -> " + outputType);
        }
    }

    private AwaitStepDescriptor loadLegacyDescriptor(String serviceName) {
        Path configPath = resolveConfigPath(serviceName);
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        PipelineYamlStep step = awaitStep(config, serviceName);
        Optional<V3AwaitTypeBinding> v3Binding = generatedV3TypeBinding(configPath, serviceName);
        if (v3Binding.isPresent()) {
            V3AwaitTypeBinding binding = v3Binding.get();
            return descriptorForStep(
                serviceName,
                step,
                binding.inputType(),
                binding.outputType(),
                binding.transportInputType(),
                binding.transportOutputType(),
                binding.inputToTransport(),
                binding.outputFromTransport());
        }
        AwaitTypeIdentities identities = authoredV3TypeIdentities(configPath, serviceName)
            .or(() -> generatedLegacyTypeIdentities(config, serviceName))
            .orElseGet(() -> new AwaitTypeIdentities(
                requiredType(step.inputType(), serviceName, "input"),
                requiredType(step.outputType(), serviceName, "output")));
        return descriptorForStep(
            serviceName,
            step,
            identities.inputType(),
            identities.outputType(),
            identities.inputType(),
            identities.outputType(),
            Function.identity(),
            Function.identity());
    }

    private static Optional<AwaitTypeIdentities> authoredV3TypeIdentities(Path configPath, String serviceName) {
        if (!isVersion3(configPath)) {
            return Optional.empty();
        }
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No version 3 await step found for generated service " + serviceName));
        String domainPackage = config.basePackage() + ".domain.";
        return Optional.of(new AwaitTypeIdentities(
            domainPackage + requiredType(step.inputTypeName(), serviceName, "input"),
            domainPackage + requiredType(step.outputTypeName(), serviceName, "output")));
    }

    private static Optional<V3AwaitTypeBinding> generatedV3TypeBinding(
        Path configPath,
        String serviceName
    ) {
        if (!isVersion3(configPath)) {
            return Optional.empty();
        }
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No version 3 await step found for generated service " + serviceName));
        String domainPackage = config.basePackage() + ".domain.";
        return generatedV3TypeBinding(
            configPath,
            serviceName,
            domainPackage + requiredType(step.inputTypeName(), serviceName, "input"),
            domainPackage + requiredType(step.outputTypeName(), serviceName, "output"));
    }

    private static Optional<V3AwaitTypeBinding> generatedV3TypeBinding(
        Path configPath,
        String serviceName,
        String requestedInputType,
        String requestedOutputType
    ) {
        if (!isVersion3(configPath)) {
            return Optional.empty();
        }
        PipelineTemplateConfig config = new PipelineTemplateConfigLoader().load(configPath);
        PipelineTemplateStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No version 3 await step found for generated service " + serviceName));
        String inputLogicalType = requiredType(step.inputTypeName(), serviceName, "input");
        String outputLogicalType = requiredType(step.outputTypeName(), serviceName, "output");
        String domainPackage = config.basePackage() + ".domain.";
        if (!requestedInputType.equals(domainPackage + inputLogicalType)
            || !requestedOutputType.equals(domainPackage + outputLogicalType)) {
            return Optional.empty();
        }
        String protoTypes = config.basePackage() + ".grpc.PipelineTypes";
        Class<?> canonicalInput = requiredGeneratedClass(domainPackage + inputLogicalType, serviceName, "canonical input");
        Class<?> canonicalOutput = requiredGeneratedClass(domainPackage + outputLogicalType, serviceName, "canonical output");
        Optional<Class<?>> transportInput = loadClass(protoTypes + "$" + inputLogicalType);
        Optional<Class<?>> transportOutput = loadClass(protoTypes + "$" + outputLogicalType);
        Optional<Class<?>> adapters = loadClass(domainPackage + "PipelineDomainProtoAdapters");
        if (transportInput.isEmpty() || transportOutput.isEmpty() || adapters.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new V3AwaitTypeBinding(
            canonicalInput.getName(),
            canonicalOutput.getName(),
            transportInput.orElseThrow().getName(),
            transportOutput.orElseThrow().getName(),
            generatedAdapter(
                adapters.orElseThrow(), "toProto", canonicalInput, transportInput.orElseThrow(), serviceName),
            generatedAdapter(
                adapters.orElseThrow(), "fromProto", transportOutput.orElseThrow(), canonicalOutput, serviceName)));
    }

    private static boolean isVersion3(Path configPath) {
        Object document = new PipelineYamlDocumentLoader().load(configPath);
        if (!(document instanceof Map<?, ?> values)) {
            return false;
        }
        Object version = values.get("version");
        if (version instanceof Number number) {
            return number.intValue() == 3;
        }
        return version != null && "3".equals(version.toString().trim());
    }

    private static Class<?> requiredGeneratedClass(String className, String serviceName, String role) {
        return loadClass(className).orElseThrow(() -> new IllegalStateException(
            "Version 3 await step " + serviceName + " cannot rebuild " + role
                + ": generated class " + className + " is unavailable"));
    }

    private static Function<Object, Object> generatedAdapter(
        Class<?> adapters,
        String methodName,
        Class<?> inputType,
        Class<?> outputType,
        String serviceName
    ) {
        Method method;
        try {
            method = adapters.getMethod(methodName, inputType);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(
                "Version 3 await step " + serviceName + " cannot rebuild generated adapter "
                    + adapters.getName() + "." + methodName + "(" + inputType.getName() + ")", e);
        }
        if (!Modifier.isStatic(method.getModifiers()) || !method.getReturnType().equals(outputType)) {
            throw new IllegalStateException(
                "Version 3 await step " + serviceName + " has incompatible generated adapter "
                    + adapters.getName() + "." + methodName + " for " + inputType.getName()
                    + " -> " + outputType.getName());
        }
        return value -> invokeGeneratedAdapter(method, value, serviceName);
    }

    private static Object invokeGeneratedAdapter(Method method, Object value, String serviceName) {
        try {
            return method.invoke(null, value);
        } catch (IllegalAccessException | InvocationTargetException e) {
            Throwable cause = e instanceof InvocationTargetException invocation && invocation.getCause() != null
                ? invocation.getCause()
                : e;
            throw new IllegalStateException(
                "Version 3 await step " + serviceName + " failed invoking generated adapter "
                    + method.getDeclaringClass().getName() + "." + method.getName(),
                cause);
        }
    }

    private static Optional<AwaitTypeIdentities> generatedLegacyTypeIdentities(
        PipelineYamlConfig config,
        String serviceName
    ) {
        if (config.basePackage() == null || config.basePackage().isBlank()) {
            return Optional.empty();
        }
        String baseName = serviceName.endsWith("Service")
            ? serviceName.substring(0, serviceName.length() - "Service".length())
            : serviceName;
        String generatedClientName = config.basePackage() + ".service.pipeline." + baseName + "AwaitClientStep";
        return loadClass(generatedClientName)
            .flatMap(AwaitStepDescriptorFactory::awaitTypeIdentities);
    }

    private static Optional<Class<?>> loadClass(String className) {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader factoryLoader = AwaitStepDescriptorFactory.class.getClassLoader();
        for (ClassLoader loader : new ClassLoader[] { contextLoader, factoryLoader }) {
            if (loader == null) {
                continue;
            }
            try {
                return Optional.of(Class.forName(className, false, loader));
            } catch (ClassNotFoundException ignored) {
                // The generated client may be absent in a runtime that only carries legacy YAML.
            }
        }
        return Optional.empty();
    }

    private static Optional<AwaitTypeIdentities> awaitTypeIdentities(Class<?> generatedClientType) {
        for (Type implementedType : generatedClientType.getGenericInterfaces()) {
            if (!(implementedType instanceof ParameterizedType parameterizedType)
                || !(parameterizedType.getRawType() instanceof Class<?> rawType)
                || !rawType.getPackageName().equals("org.pipelineframework.awaitable")
                || !rawType.getSimpleName().startsWith("Await")) {
                continue;
            }
            Type[] typeArguments = parameterizedType.getActualTypeArguments();
            if (typeArguments.length != 2
                || !(typeArguments[0] instanceof Class<?> inputType)
                || !(typeArguments[1] instanceof Class<?> outputType)) {
                continue;
            }
            return Optional.of(new AwaitTypeIdentities(inputType.getName(), outputType.getName()));
        }
        return Optional.empty();
    }

    private PipelineYamlStep awaitStep(String serviceName) {
        Path configPath = resolveConfigPath(serviceName);
        PipelineYamlConfig config = new PipelineYamlConfigLoader().load(configPath);
        return awaitStep(config, serviceName);
    }

    private static PipelineYamlStep awaitStep(PipelineYamlConfig config, String serviceName) {
        PipelineYamlStep step = config.steps().stream()
            .filter(candidate -> serviceName.equals(toServiceName(candidate.name())))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No await YAML step found for generated service " + serviceName));
        if (!"await".equalsIgnoreCase(step.kind())) {
            throw new IllegalStateException("Generated await service " + serviceName + " maps to non-await YAML step");
        }
        if (step.awaitConfig() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await configuration");
        }
        if (step.awaitConfig().transport() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await.transport configuration");
        }
        if (step.awaitConfig().correlation() == null) {
            throw new IllegalStateException("Await step " + serviceName + " is missing await.correlation configuration");
        }
        if (step.awaitConfig().correlation().strategy() == null || step.awaitConfig().correlation().strategy().isBlank()) {
            throw new IllegalArgumentException("Await step " + serviceName + " is missing await.correlation.strategy");
        }
        if (step.timeout() == null || step.timeout().isBlank()) {
            throw new IllegalArgumentException("Await step " + serviceName + " is missing timeout");
        }
        return step;
    }

    private static AwaitStepDescriptor descriptorForStep(
        String serviceName,
        PipelineYamlStep step,
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType,
        Function<Object, Object> inputToTransport,
        Function<Object, Object> outputFromTransport
    ) {
        Duration timeout;
        try {
            timeout = Duration.parse(step.timeout());
        } catch (java.time.format.DateTimeParseException ex) {
            throw new IllegalArgumentException("Await step " + serviceName + " has invalid timeout format: " + step.timeout(), ex);
        }
        Optional<PipelineYamlAwaitCompletion> completion = step.awaitConfig().completion();
        AwaitCompletionProjector<Object, Object, Object> completionProjector = completion
            .map(value -> loadCompletionProjector(serviceName, value.projector()))
            .orElseGet(() -> AwaitStepDescriptor.defaultCompletionProjector(outputFromTransport));
        String effectiveTransportOutputType = completion
            .map(PipelineYamlAwaitCompletion::type)
            .orElse(transportOutputType);
        AwaitStepDescriptor descriptor = new AwaitStepDescriptor(
            serviceName,
            inputType,
            outputType,
            step.cardinality(),
            timeout,
            step.awaitConfig().correlation().strategy(),
            step.awaitConfig().transport().type(),
            step.awaitConfig().transport().config(),
            step.idempotencyKeyFields(),
            transportInputType,
            effectiveTransportOutputType,
            inputToTransport,
            outputFromTransport,
            completion.map(PipelineYamlAwaitCompletion::projector).orElse(null),
            completionProjector,
            completion.isPresent());
        return descriptor;
    }

    @SuppressWarnings("unchecked")
    private static AwaitCompletionProjector<Object, Object, Object> loadCompletionProjector(
        String serviceName,
        String projectorClassName
    ) {
        Class<?> projectorClass = loadClass(projectorClassName).orElseThrow(() -> new IllegalArgumentException(
            "Await step " + serviceName + " could not load completion projector " + projectorClassName));
        if (!AwaitCompletionProjector.class.isAssignableFrom(projectorClass)) {
            throw new IllegalArgumentException(
                "Await step " + serviceName + " completion projector " + projectorClassName
                    + " must implement " + AwaitCompletionProjector.class.getName());
        }
        try {
            return (AwaitCompletionProjector<Object, Object, Object>) projectorClass
                .getDeclaredConstructor()
                .newInstance();
        } catch (ReflectiveOperationException exception) {
            throw new IllegalArgumentException(
                "Await step " + serviceName + " could not instantiate completion projector " + projectorClassName,
                exception);
        }
    }

    private static String requiredType(String typeName, String serviceName, String position) {
        if (typeName == null || typeName.isBlank()) {
            throw new IllegalStateException(
                "Await step " + serviceName + " is missing " + position + " type required for durable-contract reconstruction");
        }
        return typeName;
    }

    private static void ensureCompatible(
        AwaitStepDescriptor descriptor,
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType
    ) {
        if (!descriptor.inputType().equals(inputType)
            || !descriptor.outputType().equals(outputType)
            || !descriptor.transportInputType().equals(transportInputType)
            || (!descriptor.requestAwareCompletion()
                && !descriptor.transportOutputType().equals(transportOutputType))) {
            throw new IllegalStateException(
                "Conflicting await descriptor identities for stepId " + descriptor.stepId()
                    + ": canonical=" + descriptor.inputType() + " -> " + descriptor.outputType()
                    + ", transport=" + descriptor.transportInputType() + " -> " + descriptor.transportOutputType());
        }
    }

    private static Path resolveConfigPath(String serviceName) {
        String explicit = firstNonBlank(System.getProperty("pipeline.config"), System.getenv("PIPELINE_CONFIG"));
        if (explicit != null) {
            return Path.of(explicit).toAbsolutePath().normalize();
        }
        return new PipelineYamlConfigLocator().locate(Path.of("").toAbsolutePath())
            .orElseThrow(() -> new IllegalStateException("No pipeline YAML found for await step " + serviceName));
    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private static String toServiceName(String stepName) {
        if (stepName == null || stepName.isBlank()) {
            return "ProcessStepService";
        }
        String stripped = stepName.startsWith("Process ") ? stepName.substring("Process ".length()) : stepName;
        StringBuilder formatted = new StringBuilder();
        for (String part : stripped.split(" ")) {
            if (part == null || part.isBlank()) {
                continue;
            }
            String lower = part.toLowerCase(java.util.Locale.ROOT);
            formatted.append(Character.toUpperCase(lower.charAt(0)));
            if (lower.length() > 1) {
                formatted.append(lower.substring(1));
            }
        }
        return formatted.isEmpty() ? "ProcessStepService" : "Process" + formatted + "Service";
    }

    private record AwaitTypeIdentities(String inputType, String outputType) {
    }

    private record V3AwaitTypeBinding(
        String inputType,
        String outputType,
        String transportInputType,
        String transportOutputType,
        Function<Object, Object> inputToTransport,
        Function<Object, Object> outputFromTransport
    ) {
    }
}
