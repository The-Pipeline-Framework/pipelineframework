package org.pipelineframework.dispatch;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.pipelineframework.command.CommandDescriptor;
import org.pipelineframework.command.CommandIdGenerator;
import org.pipelineframework.command.CommandStepSupport;
import org.pipelineframework.command.NativeCommandSelector;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorOperationKind;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.query.NativeQuerySelector;
import org.pipelineframework.query.QueryAuthenticationRequiredException;
import org.pipelineframework.query.QueryStepDescriptor;
import org.pipelineframework.query.QueryStepSupport;
import org.pipelineframework.query.QueryTemporarilyUnavailableException;
import org.pipelineframework.query.QueryTerminalFailureException;
import org.pipelineframework.type.CanonicalTypeCatalogue;

/** Executes one compiler-authorized bound operation and returns one generic observation. */
@ApplicationScoped
public final class OperationDispatchSupport {
    private final QueryStepSupport queries;
    private final CommandStepSupport commands;
    private final java.util.function.Function<String, CommandIdGenerator<?>> commandIdGenerators;
    private final java.util.function.Function<Class<?>, CanonicalTypeCatalogue> catalogues;
    private final ObjectMapper json = PipelineJson.mapper();

    @Inject
    public OperationDispatchSupport(
        QueryStepSupport queries,
        CommandStepSupport commands,
        Instance<CommandIdGenerator<?>> commandIdGenerators
    ) {
        this(queries, commands, className -> selectGenerator(commandIdGenerators, className),
            type -> CanonicalTypeCatalogue.load(classLoader(type)));
    }

    public OperationDispatchSupport(
        QueryStepSupport queries,
        CommandStepSupport commands,
        java.util.function.Function<String, CommandIdGenerator<?>> commandIdGenerators
    ) {
        this(queries, commands, commandIdGenerators,
            type -> CanonicalTypeCatalogue.load(classLoader(type)));
    }

    OperationDispatchSupport(
        QueryStepSupport queries,
        CommandStepSupport commands,
        java.util.function.Function<String, CommandIdGenerator<?>> commandIdGenerators,
        java.util.function.Function<Class<?>, CanonicalTypeCatalogue> catalogues
    ) {
        this.queries = Objects.requireNonNull(queries, "Query support must not be null");
        this.commands = Objects.requireNonNull(commands, "Command support must not be null");
        this.commandIdGenerators = Objects.requireNonNull(commandIdGenerators, "command ID generator resolver must not be null");
        this.catalogues = Objects.requireNonNull(catalogues, "canonical catalogue resolver must not be null");
    }

    public <O> Uni<O> dispatch(
        OperationDispatchDescriptor descriptor,
        String binding,
        String operation,
        String argumentsJson,
        Class<O> observationType
    ) {
        Objects.requireNonNull(descriptor, "dispatch descriptor must not be null");
        Objects.requireNonNull(observationType, "observation type must not be null");
        DispatchCapability capability = descriptor.require(binding, operation);
        CanonicalTypeCatalogue catalogue = catalogues.apply(observationType);
        final String canonicalArguments;
        final Object input;
        try {
            canonicalArguments = catalogue.validateAndCanonicalize(capability.inputType(), argumentsJson);
            input = json.readValue(canonicalArguments, capability.inputClass());
        } catch (Exception failure) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                "invalid arguments for exposed operation " + binding + "/" + operation, failure));
        }
        if (ConnectorOperationKind.QUERY.equals(capability.identity().kind())) {
            return dispatchQuery(descriptor, capability, input, catalogue, observationType);
        }
        return dispatchCommand(descriptor, capability, input, catalogue, observationType);
    }

    private <O> Uni<O> dispatchQuery(
        OperationDispatchDescriptor dispatch,
        DispatchCapability capability,
        Object input,
        CanonicalTypeCatalogue catalogue,
        Class<O> observationType
    ) {
        QueryStepDescriptor descriptor = QueryStepDescriptor.nativeQuery(
            dispatch.stepId() + ":" + capability.reference().binding().value() + "/" + capability.reference().operation(),
            capability.inputClass().getName(),
            capability.outputClass().getName(),
            "ONE_TO_ONE",
            new NativeQuerySelector(capability.reference().binding(), capability.identity(), capability.providerMajorVersion()),
            capability.configuration(),
            capability.queryCapabilities().orElseThrow(),
            Optional.empty());
        return queries.queryOutcomeOneToOne(descriptor, input, capability.outputClass())
            .onItem().transformToUni(outcome -> queryObservation(capability, outcome, catalogue, observationType));
    }

    private <O> Uni<O> queryObservation(
        DispatchCapability capability,
        QueryOutcome<?> outcome,
        CanonicalTypeCatalogue catalogue,
        Class<O> observationType
    ) {
        if (outcome instanceof QueryOutcome.Found<?> found) {
            return Uni.createFrom().item(resultObservation(
                capability, "found", outcome.code(), found.output(), catalogue, observationType));
        }
        if (outcome instanceof QueryOutcome.NotFound<?>) {
            return Uni.createFrom().item(emptyObservation(capability, "not-found", outcome.code(), observationType));
        }
        if (outcome instanceof QueryOutcome.TemporarilyUnavailable<?>) {
            return Uni.createFrom().failure(new QueryTemporarilyUnavailableException(outcome.code()));
        }
        if (outcome instanceof QueryOutcome.AuthenticationRequired<?>) {
            return Uni.createFrom().failure(new QueryAuthenticationRequiredException(outcome.code()));
        }
        if (outcome instanceof QueryOutcome.TerminalFailure<?>) {
            return Uni.createFrom().failure(new QueryTerminalFailureException(outcome.code()));
        }
        return Uni.createFrom().failure(new IllegalStateException("unsupported Query outcome " + outcome));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <O> Uni<O> dispatchCommand(
        OperationDispatchDescriptor dispatch,
        DispatchCapability capability,
        Object input,
        CanonicalTypeCatalogue catalogue,
        Class<O> observationType
    ) {
        DispatchCapability.CommandConfiguration command = capability.commandConfiguration().orElseThrow();
        CommandIdGenerator generator = commandIdGenerators.apply(command.commandIdGenerator());
        NativeCommandSelector selector = new NativeCommandSelector(
            Optional.of(capability.reference().binding()), capability.identity(), capability.providerMajorVersion(), command.policy());
        CommandDescriptor descriptor = CommandDescriptor.nativeCommand(
            dispatch.stepId() + ":" + capability.reference().binding().value() + "/" + capability.reference().operation(),
            selector,
            capability.inputClass().getName(),
            capability.outputClass().getName(),
            command.commandIdGenerator(),
            command.duplicatePolicy(),
            capability.configuration());
        return commands.execute(descriptor, generator, input)
            .onItem().transform(output -> resultObservation(
                capability, "succeeded", "succeeded", output, catalogue, observationType));
    }

    private <O> O resultObservation(
        DispatchCapability capability,
        String outcome,
        String code,
        Object result,
        CanonicalTypeCatalogue catalogue,
        Class<O> observationType
    ) {
        try {
            String resultJson = catalogue.validateAndCanonicalize(
                capability.outputType(), json.writeValueAsString(result));
            Object payload = instantiatePayload(observationType, "OperationResultObservation", new Object[] {
                capability.reference().binding().value(), capability.reference().operation(), capability.identity().kind().value(),
                capability.identity().majorVersion(), outcome, code, capability.outputType(), resultJson});
            return instantiateVariant(observationType, "Result", payload);
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("failed to materialize operation result observation", failure);
        }
    }

    private <O> O emptyObservation(
        DispatchCapability capability,
        String outcome,
        String code,
        Class<O> observationType
    ) {
        try {
            Object payload = instantiatePayload(observationType, "OperationEmptyObservation", new Object[] {
                capability.reference().binding().value(), capability.reference().operation(), capability.identity().kind().value(),
                capability.identity().majorVersion(), outcome, code});
            return instantiateVariant(observationType, "Empty", payload);
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw new IllegalStateException("failed to materialize empty operation observation", failure);
        }
    }

    private static Object instantiatePayload(Class<?> observationType, String name, Object[] arguments) throws Exception {
        Class<?> payload = Class.forName(observationType.getPackageName() + "." + name, true, observationType.getClassLoader());
        Constructor<?> constructor = java.util.Arrays.stream(payload.getDeclaredConstructors())
            .filter(candidate -> candidate.getParameterCount() == arguments.length)
            .findFirst().orElseThrow(() -> new IllegalStateException("no canonical constructor for " + name));
        return constructor.newInstance(arguments);
    }

    private static <O> O instantiateVariant(Class<O> observationType, String name, Object payload) throws Exception {
        Class<?> variant = java.util.Arrays.stream(observationType.getDeclaredClasses())
            .filter(candidate -> candidate.getSimpleName().equals(name))
            .findFirst().orElseThrow(() -> new IllegalStateException("no observation variant " + name));
        Constructor<?> constructor = java.util.Arrays.stream(variant.getDeclaredConstructors())
            .filter(candidate -> candidate.getParameterCount() == 1)
            .findFirst().orElseThrow(() -> new IllegalStateException("no unary observation variant " + name));
        return observationType.cast(constructor.newInstance(payload));
    }

    private static ClassLoader classLoader(Class<?> type) {
        ClassLoader loader = type.getClassLoader();
        return loader == null ? OperationDispatchSupport.class.getClassLoader() : loader;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static CommandIdGenerator<?> selectGenerator(Instance<CommandIdGenerator<?>> generators, String className) {
        try {
            Class<?> type = Class.forName(className, true, OperationDispatchSupport.class.getClassLoader());
            Instance selected = generators.select((Class) type);
            if (selected.isUnsatisfied()) {
                throw new IllegalStateException("no CDI command ID generator bean found for " + className);
            }
            if (selected.isAmbiguous()) {
                throw new IllegalStateException("multiple CDI command ID generator beans found for " + className);
            }
            return (CommandIdGenerator<?>) selected.get();
        } catch (ClassNotFoundException failure) {
            throw new IllegalStateException("command ID generator class not found: " + className, failure);
        }
    }
}
