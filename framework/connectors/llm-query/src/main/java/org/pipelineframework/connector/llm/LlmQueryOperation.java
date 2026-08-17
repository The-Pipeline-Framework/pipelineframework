package org.pipelineframework.connector.llm;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.pipelineframework.config.pipeline.PipelineJson;
import org.pipelineframework.connector.ConnectorConfigSchema;
import org.pipelineframework.connector.QueryInvocation;
import org.pipelineframework.connector.QueryOperation;
import org.pipelineframework.connector.QueryOutcome;

/** Exactly-one-inference Query operation. Proposed calls remain inert typed data. */
final class LlmQueryOperation implements QueryOperation<Object, LlmTurnConfiguration, Object> {
    private static final ConnectorConfigSchema<LlmTurnConfiguration> CONFIGURATION_SCHEMA =
        ConnectorConfigSchema.record(LlmTurnConfiguration.class, "llm.query.turn", 1);
    private static final ObjectMapper JSON = PipelineJson.mapper();
    private final Supplier<Optional<LlmDecisionClient>> client;

    LlmQueryOperation(Supplier<Optional<LlmDecisionClient>> client) {
        this.client = Objects.requireNonNull(client, "LLM decision client supplier must not be null");
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
        final LlmTurnRequest request;
        try {
            CanonicalTypeCatalogue catalogue = CanonicalTypeCatalogue.load(classLoader(invocation.outputType()));
            contract = DecisionContract.from(invocation.outputType(), invocation.configuration(), catalogue);
            request = new LlmTurnRequest(
                invocation.configuration().instructions(),
                JSON.writeValueAsString(invocation.input()),
                contract.tools());
        } catch (Exception failure) {
            return CompletableFuture.failedStage(failure);
        }
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
                return new QueryOutcome.TerminalFailure<>("invalid-model-decision");
            }
        });
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
        CanonicalTypeCatalogue catalogue,
        Map<String, LlmCallableConfiguration> callables,
        Map<String, String> completionVariants,
        String callDiscriminator,
        List<LlmToolDefinition> tools
    ) {
        private DecisionContract {
            callables = Map.copyOf(callables);
            completionVariants = Map.copyOf(completionVariants);
            tools = List.copyOf(tools);
        }

        static DecisionContract from(
            Class<?> outputType,
            LlmTurnConfiguration configuration,
            CanonicalTypeCatalogue catalogue
        ) {
            String outputName = outputType.getSimpleName();
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
            if (configuration.callables().isEmpty() && completions.isEmpty()) {
                throw new IllegalStateException("LLM Query has no callable or completion alternative");
            }
            List<LlmToolDefinition> tools = new ArrayList<>();
            configuration.callables().entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
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
                outputType, catalogue, configuration.callables(), completions, callDiscriminator, tools);
        }

        Object materialize(LlmToolProposal proposal) {
            if (proposal == null || proposal.alias().isBlank()) {
                throw new InvalidModelDecisionException("model did not select a tool alias");
            }
            LlmCallableConfiguration callable = callables.get(proposal.alias());
            if (callable != null) {
                String arguments = catalogue.validateAndCanonicalize(callable.input(), proposal.argumentsJson());
                Object agentCall = instantiateAgentCall(callable, arguments);
                return instantiateVariant(callDiscriminator, agentCall);
            }
            String completionType = completionVariants.get(proposal.alias());
            if (completionType == null) {
                throw new InvalidModelDecisionException("model selected unknown tool alias '" + proposal.alias() + "'");
            }
            String arguments = catalogue.validateAndCanonicalize(completionType, proposal.argumentsJson());
            try {
                Class<?> payloadType = Class.forName(outputType.getPackageName() + "." + completionType, true,
                    outputType.getClassLoader());
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
                    outputType.getClassLoader());
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
                    .filter(candidate -> candidate.getParameterCount() == 1)
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
