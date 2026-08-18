package org.pipelineframework.connector.llm;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.MaterializedPayload;
import org.pipelineframework.connector.PayloadMaterializer;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;
import org.pipelineframework.type.CanonicalTypeCatalogue;
import org.pipelineframework.repository.PayloadReference;

/**
 * Exactly-one-inference Query operation. Proposed calls remain inert typed data.
 *
 * <p>A non-union output selects direct-completion mode: the model receives one required
 * {@code complete} tool whose schema is the application-authored output type, and the callable
 * catalogue must be empty. Union outputs retain the AgentCall-based callable/completion contract.</p>
 */
final class LlmQueryOperation implements QueryOperation<Object, LlmTurnConfiguration, Object> {
    private static final long MAX_PAYLOAD_BYTES = 20L * 1024L * 1024L;
    private static final ConnectorConfigSchema<LlmTurnConfiguration> CONFIGURATION_SCHEMA =
        ConnectorConfigSchema.record(LlmTurnConfiguration.class, "llm.query.turn", 1);
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private static final System.Logger LOG = System.getLogger(LlmQueryOperation.class.getName());
    private final Supplier<Optional<LlmDecisionClient>> client;
    private final Function<ClassLoader, CanonicalTypeCatalogue> catalogueLoader;
    private final Map<DecisionContractKey, DecisionContract> contracts = new ConcurrentHashMap<>();

    LlmQueryOperation(Supplier<Optional<LlmDecisionClient>> client) {
        this(client, CanonicalTypeCatalogue::load);
    }

    LlmQueryOperation(
        Supplier<Optional<LlmDecisionClient>> client,
        Function<ClassLoader, CanonicalTypeCatalogue> catalogueLoader
    ) {
        this.client = Objects.requireNonNull(client, "LLM decision client supplier must not be null");
        this.catalogueLoader = Objects.requireNonNull(catalogueLoader, "canonical catalogue loader must not be null");
    }

    @Override
    public String id() {
        return "decide";
    }

    @Override
    public Optional<ConnectorConfigSchema<LlmTurnConfiguration>> configurationSchema() {
        return Optional.of(CONFIGURATION_SCHEMA);
    }

    @Override
    public CompletionStage<QueryOutcome<Object>> query(QueryInvocation<Object, LlmTurnConfiguration, Object> invocation) {
        LlmDecisionClient active = client.get().orElse(null);
        if (active == null) {
            return CompletableFuture.failedStage(new IllegalStateException("LLM Query binding is not active"));
        }
        final DecisionContract contract;
        final String applicationStateJson;
        try {
            DecisionContractKey key = new DecisionContractKey(invocation.outputType(), invocation.configuration());
            contract = contracts.computeIfAbsent(key, binding -> {
                ClassLoader loader = classLoader(binding.outputType());
                return DecisionContract.from(
                    binding.outputType(), binding.configuration(), catalogueLoader.apply(loader), loader);
            });
            applicationStateJson = JSON.writeValueAsString(invocation.input());
        } catch (Exception failure) {
            return CompletableFuture.failedStage(failure);
        }
        if (invocation.configuration().structuredOutputMode() == StructuredOutputSchemaMode.REQUIRED
            && !active.supportsNativeStructuredOutput()) {
            return CompletableFuture.completedStage(new QueryOutcome.TerminalFailure<>("structured-output-unavailable"));
        }
        return CompletableFuture.completedStage(invocation)
            .thenCompose(this::materializePayloads)
            .thenCompose(media -> decide(
                active,
                new LlmTurnRequest(
                    invocation.configuration().instructions(),
                    applicationStateJson,
                    media,
                    contract.tools(),
                    invocation.configuration().structuredOutputMode()),
                contract))
            .exceptionally(failure -> {
                LOG.log(System.Logger.Level.WARNING,
                    "LLM Query failed without a repair or retry inference", failure);
                return new QueryOutcome.TerminalFailure<>("llm-query-failed");
            });
    }

    private CompletionStage<QueryOutcome<Object>> decide(
        LlmDecisionClient active,
        LlmTurnRequest request,
        DecisionContract contract
    ) {
        CompletionStage<LlmToolProposal> decision;
        try {
            decision = Objects.requireNonNull(active.decide(request), "LLM adapter returned a null decision stage");
        } catch (RuntimeException failure) {
            return CompletableFuture.failedStage(failure);
        }
        return decision.thenApply(proposal -> {
            try {
                return new QueryOutcome.Found<>(contract.materialize(proposal));
            } catch (InvalidModelDecisionException failure) {
                LOG.log(System.Logger.Level.WARNING,
                    "LLM Query rejected an invalid model decision: " + failure.getMessage(), failure);
                return new QueryOutcome.TerminalFailure<>("invalid-model-decision");
            }
        });
    }

    private CompletionStage<List<MaterializedPayload>> materializePayloads(
        QueryInvocation<Object, LlmTurnConfiguration, Object> invocation
    ) {
        List<PayloadReference> references = payloadReferences(invocation.input());
        if (references.isEmpty()) {
            return CompletableFuture.completedStage(List.of());
        }
        PayloadMaterializer materializer = invocation.payloadMaterializer().orElseThrow(() ->
            new IllegalStateException("LLM Query input contains payload references but no materializer is available"));
        CompletionStage<MaterializationBatch> stage = CompletableFuture.completedStage(
            new MaterializationBatch(List.of(), 0));
        for (PayloadReference reference : references) {
            stage = stage.thenCompose(batch -> {
                if (reference.sizeBytes() > MAX_PAYLOAD_BYTES) {
                    return CompletableFuture.failedStage(new IllegalStateException(
                        "LLM Query payload exceeds the 20 MiB materialization limit"));
                }
                return materializer.materialize(reference, MAX_PAYLOAD_BYTES).thenApply(payload -> {
                    long totalBytes = Math.addExact(batch.totalBytes(), payload.bytes().length);
                    if (totalBytes > MAX_PAYLOAD_BYTES) {
                        throw new IllegalStateException(
                            "LLM Query payloads exceed the 20 MiB materialization limit");
                    }
                    List<MaterializedPayload> next = new ArrayList<>(batch.payloads());
                    next.add(payload);
                    return new MaterializationBatch(List.copyOf(next), totalBytes);
                });
            });
        }
        return stage.thenApply(MaterializationBatch::payloads);
    }

    private record MaterializationBatch(List<MaterializedPayload> payloads, long totalBytes) { }

    private static List<PayloadReference> payloadReferences(Object input) {
        LinkedHashSet<PayloadReference> references = new LinkedHashSet<>();
        collectPayloadReferences(input, references, java.util.Collections.newSetFromMap(new IdentityHashMap<>()));
        return List.copyOf(references);
    }

    private static void collectPayloadReferences(
        Object value,
        LinkedHashSet<PayloadReference> references,
        java.util.Set<Object> visited
    ) {
        if (value == null || value instanceof String || value instanceof Number
            || value instanceof Boolean || value instanceof Character || value.getClass().isEnum()) {
            return;
        }
        if (value instanceof PayloadReference reference) {
            references.add(reference);
            return;
        }
        if (!visited.add(value)) {
            return;
        }
        if (value instanceof Optional<?> optional) {
            optional.ifPresent(item -> collectPayloadReferences(item, references, visited));
            return;
        }
        if (value instanceof Iterable<?> iterable) {
            iterable.forEach(item -> collectPayloadReferences(item, references, visited));
            return;
        }
        if (value instanceof Map<?, ?> map) {
            map.values().forEach(item -> collectPayloadReferences(item, references, visited));
            return;
        }
        if (!value.getClass().isRecord()) {
            return;
        }
        for (RecordComponent component : value.getClass().getRecordComponents()) {
            try {
                collectPayloadReferences(component.getAccessor().invoke(value), references, visited);
            } catch (IllegalAccessException | InvocationTargetException failure) {
                throw new IllegalStateException(
                    "Failed inspecting payload reference field '" + component.getName() + "'", failure);
            }
        }
    }

    private record DecisionContractKey(Class<?> outputType, LlmTurnConfiguration configuration) {
        private DecisionContractKey {
            Objects.requireNonNull(outputType, "LLM output type must not be null");
            Objects.requireNonNull(configuration, "LLM turn configuration must not be null");
        }
    }

    private static ClassLoader classLoader(Class<?> outputType) {
        ClassLoader loader = outputType.getClassLoader();
        if (loader != null) {
            return loader;
        }
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        return context == null ? LlmQueryOperation.class.getClassLoader() : context;
    }

    private record DecisionContract(
        Class<?> outputType,
        ClassLoader classLoader,
        CanonicalTypeCatalogue catalogue,
        Map<String, LlmCallableConfiguration> callables,
        Map<String, String> completionVariants,
        Optional<String> callDiscriminator,
        Optional<String> directCompletionType,
        List<LlmToolDefinition> tools
    ) {
        private DecisionContract {
            callables = Map.copyOf(callables);
            completionVariants = Map.copyOf(completionVariants);
            callDiscriminator = Objects.requireNonNull(callDiscriminator, "call discriminator must not be null");
            directCompletionType = Objects.requireNonNull(
                directCompletionType, "direct completion type must not be null");
            tools = List.copyOf(tools);
        }

        static DecisionContract from(
            Class<?> outputType,
            LlmTurnConfiguration configuration,
            CanonicalTypeCatalogue catalogue,
            ClassLoader classLoader
        ) {
            String outputName = outputType.getSimpleName();
            if (!catalogue.isUnion(outputName)) {
                if (!configuration.callableCatalogue().isEmpty()) {
                    throw new IllegalStateException(
                        "LLM Query callables require an output union containing <tpf.llm.AgentCall>");
                }
                return new DecisionContract(
                    outputType,
                    classLoader,
                    catalogue,
                    Map.of(),
                    Map.of(),
                    Optional.empty(),
                    Optional.of(outputName),
                    List.of(new LlmToolDefinition(
                        "complete", "Complete with " + outputName, catalogue.schema(outputName))));
            }
            Map<String, String> variants = catalogue.unionVariants(outputName);
            List<String> callVariants = variants.entrySet().stream()
                .filter(entry -> catalogue.contributedIdentity(entry.getValue())
                    .filter(LlmProtocolTypeContributor.AGENT_CALL.qualifiedName()::equals).isPresent())
                .map(Map.Entry::getKey)
                .toList();
            if (callVariants.size() != 1) {
                throw new IllegalStateException("LLM Query output union '" + outputName
                    + "' must declare exactly one <tpf.llm.AgentCall> variant");
            }
            String callDiscriminator = callVariants.getFirst();
            Map<String, String> completions = new LinkedHashMap<>();
            variants.forEach((discriminator, payload) -> {
                if (!discriminator.equals(callDiscriminator)) {
                    completions.put(discriminator, payload);
                }
            });
            if (configuration.callableCatalogue().isEmpty() && completions.isEmpty()) {
                throw new IllegalStateException("LLM Query has no callable or completion alternative");
            }
            List<LlmToolDefinition> tools = new ArrayList<>();
            configuration.callableCatalogue().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
                if (completions.containsKey(entry.getKey())) {
                    throw new IllegalStateException("LLM callable alias '" + entry.getKey()
                        + "' conflicts with completion discriminator");
                }
                LlmCallableConfiguration callable = entry.getValue();
                tools.add(new LlmToolDefinition(
                    entry.getKey(),
                    "Propose " + callable.using() + "/" + callable.operation(),
                    catalogue.schema(callable.input())));
            });
            completions.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry ->
                tools.add(new LlmToolDefinition(
                    entry.getKey(), "Complete with " + entry.getValue(), catalogue.schema(entry.getValue()))));
            return new DecisionContract(
                outputType,
                classLoader,
                catalogue,
                configuration.callableCatalogue(),
                completions,
                Optional.of(callDiscriminator),
                Optional.empty(),
                tools);
        }

        Object materialize(LlmToolProposal proposal) {
            if (proposal == null || proposal.alias().isBlank()) {
                throw new InvalidModelDecisionException("model did not select a tool alias");
            }
            if (directCompletionType.isPresent()) {
                if (!"complete".equals(proposal.alias())) {
                    throw new InvalidModelDecisionException(
                        "model selected unknown tool alias '" + proposal.alias() + "'");
                }
                try {
                    String arguments = catalogue.validateAndCanonicalize(
                        directCompletionType.orElseThrow(), proposal.argumentsJson());
                    return JSON.readValue(arguments, outputType);
                } catch (InvalidModelDecisionException failure) {
                    throw failure;
                } catch (Exception failure) {
                    throw new InvalidModelDecisionException("completion payload cannot be materialized", failure);
                }
            }
            LlmCallableConfiguration callable = callables.get(proposal.alias());
            if (callable != null) {
                try {
                    String arguments = catalogue.validateAndCanonicalize(callable.input(), proposal.argumentsJson());
                    Object agentCall = instantiateAgentCall(callable, arguments);
                    return instantiateVariant(callDiscriminator.orElseThrow(), agentCall);
                } catch (InvalidModelDecisionException failure) {
                    throw failure;
                } catch (IllegalArgumentException failure) {
                    throw new InvalidModelDecisionException(failure.getMessage(), failure);
                }
            }
            String completionType = completionVariants.get(proposal.alias());
            if (completionType == null) {
                throw new InvalidModelDecisionException("model selected unknown tool alias '" + proposal.alias() + "'");
            }
            try {
                String arguments = catalogue.validateAndCanonicalize(completionType, proposal.argumentsJson());
                Class<?> payloadType = Class.forName(outputType.getPackageName() + "." + completionType, true,
                    classLoader);
                return instantiateVariant(proposal.alias(), JSON.readValue(arguments, payloadType));
            } catch (InvalidModelDecisionException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new InvalidModelDecisionException("completion payload cannot be materialized", failure);
            }
        }

        private Object instantiateAgentCall(LlmCallableConfiguration callable, String arguments) {
            try {
                Class<?> agentCall = Class.forName(outputType.getPackageName() + ".AgentCall", true,
                    classLoader);
                Constructor<?> constructor = agentCall.getDeclaredConstructor(String.class, String.class, String.class);
                return constructor.newInstance(callable.using(), callable.operation(), arguments);
            } catch (Exception failure) {
                throw new InvalidModelDecisionException("AgentCall payload cannot be materialized", failure);
            }
        }

        private Object instantiateVariant(String discriminator, Object payload) {
            String variantName = Character.toUpperCase(discriminator.charAt(0)) + discriminator.substring(1);
            Class<?> variant = List.of(outputType.getDeclaredClasses()).stream()
                .filter(candidate -> candidate.getSimpleName().equals(variantName))
                .findFirst()
                .orElseThrow(() -> new InvalidModelDecisionException(
                    "output union has no runtime variant for discriminator '" + discriminator + "'"));
            try {
                Constructor<?> constructor = List.of(variant.getDeclaredConstructors()).stream()
                    .filter(candidate -> candidate.getParameterCount() == 1
                        && candidate.getParameterTypes()[0].isInstance(payload))
                    .findFirst()
                    .orElseThrow(() -> new InvalidModelDecisionException(
                        "output union variant '" + discriminator + "' has no unary constructor"));
                return constructor.newInstance(payload);
            } catch (InvalidModelDecisionException failure) {
                throw failure;
            } catch (Exception failure) {
                throw new InvalidModelDecisionException(
                    "output union variant '" + discriminator + "' cannot be materialized", failure);
            }
        }
    }
}
